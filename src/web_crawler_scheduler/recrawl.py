"""Re-rastreo según la tasa de cambio de cada página, con peticiones
condicionales (`ETag` / `Last-Modified`).

## Cuándo volver a una página

Cada página tiene su propia tasa de cambio estimada `λ` (cambios por
segundo). Se modela como un proceso de Poisson, el modelo clásico de Cho y
Garcia-Molina para la frescura de un crawler: con `n` visitas en las que se
detectaron `X` cambios, el estimador con corrección de sesgo es

    λ̂ = -ln((n - X + 0.5) / (n + 0.5)) / Ī

donde `Ī` es el intervalo medio entre visitas. A diferencia de `X / tiempo`,
este estimador no infravalora páginas que cambian varias veces entre dos
visitas (solo vemos "cambió o no", nunca cuántas veces). El siguiente
intervalo es el que da una probabilidad `target_change_probability` de que la
página haya cambiado al volver: `P(cambio en I) = 1 - e^(-λI)`, así que
`I = -ln(1 - p) / λ̂`, acotado a `[min_interval, max_interval]`. Una página que
nunca cambia se aleja hasta `max_interval`; una que cambia en cada visita se
acerca hasta `min_interval`.

## Cómo saber si cambió

- Si la última respuesta traía `ETag` o `Last-Modified`, la petición es
  condicional y un `304 Not Modified` confirma "sin cambios" sin descargar
  el cuerpo.
- Si el servidor no da validadores (o los ignora y responde 200), se compara
  el SHA-256 del cuerpo con el de la versión anterior.
- Un 404 o 410 se trata como página desaparecida: deja de re-rastrearse y se
  emite en `removed.jsonl` para que el índice la borre.

Las páginas cambiadas se escriben en el mismo `pages.jsonl` que produce el
crawl normal (`models.PageRecord`), así que la extracción y la indexación
incremental no necesitan saber que vienen de un re-rastreo. Robots.txt y el
rate limiting por dominio son los mismos componentes que usa `CrawlPipeline`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Any, Protocol

from web_crawler_scheduler.fetcher import FetchError
from web_crawler_scheduler.models import CacheValidators, FetchResult, PageRecord
from web_crawler_scheduler.protocols import RateLimiter, RobotsPolicy

logger = logging.getLogger(__name__)

RECRAWL_STATE_FORMAT_VERSION = 1
_GONE_STATUSES = frozenset({HTTPStatus.NOT_FOUND, HTTPStatus.GONE})


class ConditionalPageFetcher(Protocol):
    async def fetch(
        self, url: str, timeout_seconds: float, validators: CacheValidators | None = None
    ) -> FetchResult: ...


@dataclass(frozen=True, slots=True)
class ChangeRatePolicy:
    initial_interval: timedelta = timedelta(days=1)
    min_interval: timedelta = timedelta(hours=1)
    max_interval: timedelta = timedelta(days=30)
    target_change_probability: float = 0.5
    max_growth_factor: float = 2.0
    """El intervalo nuevo no supera `max_growth_factor` veces el último. Con
    pocas visitas sin cambios el estimador da `λ̂ = 0` y saltaría de golpe a
    `max_interval`; así una página estable se aleja de forma gradual
    (1 día, 2, 4...) y un cambio aislado no la deja olvidada un mes."""

    def __post_init__(self) -> None:
        if self.min_interval <= timedelta(0):
            raise ValueError("min_interval debe ser positivo")
        if not self.min_interval <= self.initial_interval <= self.max_interval:
            raise ValueError("debe cumplirse min_interval <= initial_interval <= max_interval")
        if not 0.0 < self.target_change_probability < 1.0:
            raise ValueError("target_change_probability debe estar en (0, 1)")
        if self.max_growth_factor < 1.0:
            raise ValueError("max_growth_factor debe ser >= 1")

    def estimated_change_rate(self, checks: int, changes: int, mean_interval: float) -> float:
        """Cambios por segundo (estimador de Cho y Garcia-Molina)."""
        if checks <= 0 or mean_interval <= 0:
            raise ValueError("hacen falta visitas y un intervalo medio positivo")
        ratio = (checks - changes + 0.5) / (checks + 0.5)
        return -math.log(ratio) / mean_interval

    def next_interval(
        self,
        checks: int,
        changes: int,
        total_interval_seconds: float,
        last_interval_seconds: float | None = None,
    ) -> timedelta:
        if checks == 0 or total_interval_seconds <= 0:
            return self.initial_interval
        rate = self.estimated_change_rate(checks, changes, total_interval_seconds / checks)
        # λ̂ = 0 (ningún cambio observado) equivale a "nunca cambia": intervalo
        # máximo, que el tope de crecimiento de abajo convierte en gradual.
        seconds = (
            -math.log(1.0 - self.target_change_probability) / rate
            if rate > 0.0
            else self.max_interval.total_seconds()
        )
        if last_interval_seconds is not None and last_interval_seconds > 0:
            seconds = min(seconds, last_interval_seconds * self.max_growth_factor)
        bounded = min(
            max(seconds, self.min_interval.total_seconds()), self.max_interval.total_seconds()
        )
        return timedelta(seconds=bounded)


@dataclass(frozen=True, slots=True)
class RecrawlState:
    url: str
    depth: int
    etag: str | None
    last_modified: str | None
    content_sha256: str | None
    last_fetched_at: datetime
    next_due_at: datetime
    checks: int = 0
    """Re-rastreos completados (la descarga inicial no cuenta)."""
    changes: int = 0
    total_interval_seconds: float = 0.0
    """Suma de los intervalos entre visitas consecutivas, para `Ī`."""

    @property
    def validators(self) -> CacheValidators:
        return CacheValidators(etag=self.etag, last_modified=self.last_modified)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "depth": self.depth,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "content_sha256": self.content_sha256,
            "last_fetched_at": self.last_fetched_at.isoformat(),
            "next_due_at": self.next_due_at.isoformat(),
            "checks": self.checks,
            "changes": self.changes,
            "total_interval_seconds": self.total_interval_seconds,
        }

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> RecrawlState:
        return RecrawlState(
            url=str(data["url"]),
            depth=int(data["depth"]),
            etag=None if data["etag"] is None else str(data["etag"]),
            last_modified=None if data["last_modified"] is None else str(data["last_modified"]),
            content_sha256=None if data["content_sha256"] is None else str(data["content_sha256"]),
            last_fetched_at=datetime.fromisoformat(str(data["last_fetched_at"])),
            next_due_at=datetime.fromisoformat(str(data["next_due_at"])),
            checks=int(data["checks"]),
            changes=int(data["changes"]),
            total_interval_seconds=float(data["total_interval_seconds"]),
        )


def content_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class RecrawlStore:
    """Estado de re-rastreo de todas las páginas conocidas, persistido en un
    único JSON (`{"format_version": 1, "pages": [...]}`, ordenado por URL)
    reemplazado de forma atómica en cada `save`."""

    def __init__(self, states: Iterable[RecrawlState] = ()) -> None:
        self._states: dict[str, RecrawlState] = {state.url: state for state in states}

    def __len__(self) -> int:
        return len(self._states)

    def get(self, url: str) -> RecrawlState | None:
        return self._states.get(url)

    def put(self, state: RecrawlState) -> None:
        self._states[state.url] = state

    def remove(self, url: str) -> None:
        self._states.pop(url, None)

    def due(self, now: datetime, limit: int | None = None) -> list[RecrawlState]:
        """Páginas vencidas, la más atrasada primero (desempate por URL)."""
        due = sorted(
            (state for state in self._states.values() if state.next_due_at <= now),
            key=lambda state: (state.next_due_at, state.url),
        )
        return due if limit is None else due[:limit]

    def seed_from_pages(self, pages_path: Path, policy: ChangeRatePolicy) -> int:
        """Registra cada página de un `pages.jsonl` de crawl como punto de
        partida (validadores y hash de su descarga). Una URL ya conocida no
        se pisa. Devuelve cuántas páginas nuevas se añadieron."""
        added = 0
        with pages_path.open("r", encoding="utf-8") as pages_file:
            for line_number, raw_line in enumerate(pages_file, start=1):
                if not raw_line.strip():
                    continue
                try:
                    page = PageRecord.from_json_dict(json.loads(raw_line))
                except (json.JSONDecodeError, KeyError, ValueError) as error:
                    logger.warning(
                        "%s:%d: página ilegible, se omite (%s)", pages_path, line_number, error
                    )
                    continue
                if page.url in self._states:
                    continue
                validators = CacheValidators.from_headers(page.headers)
                self._states[page.url] = RecrawlState(
                    url=page.url,
                    depth=page.depth,
                    etag=validators.etag,
                    last_modified=validators.last_modified,
                    content_sha256=content_sha256(page.html),
                    last_fetched_at=page.fetched_at,
                    next_due_at=page.fetched_at + policy.initial_interval,
                )
                added += 1
        return added

    def save(self, path: Path) -> None:
        payload = {
            "format_version": RECRAWL_STATE_FORMAT_VERSION,
            "pages": [self._states[url].to_json_dict() for url in sorted(self._states)],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)

    @staticmethod
    def load(path: Path) -> RecrawlStore:
        data = json.loads(path.read_text(encoding="utf-8"))
        version = int(data.get("format_version", -1))
        if version != RECRAWL_STATE_FORMAT_VERSION:
            raise ValueError(f"estado de re-rastreo no soportado: version {version}")
        return RecrawlStore(RecrawlState.from_json_dict(entry) for entry in data["pages"])


@dataclass(frozen=True, slots=True)
class RecrawlReport:
    checked: int
    not_modified: int
    """304: el servidor confirmó que no cambió."""
    unchanged_body: int
    """200 con el mismo SHA-256 que la versión anterior."""
    changed: int
    removed: int
    failed: int
    robots_disallowed: int


@dataclass(slots=True)
class _Counters:
    not_modified: int = 0
    unchanged_body: int = 0
    changed: int = 0
    removed: int = 0
    failed: int = 0
    robots_disallowed: int = 0


class RecrawlPipeline:
    """Revisita las páginas vencidas de un `RecrawlStore`."""

    def __init__(
        self,
        store: RecrawlStore,
        fetcher: ConditionalPageFetcher,
        rate_limiter: RateLimiter,
        robots: RobotsPolicy | None,
        *,
        policy: ChangeRatePolicy,
        user_agent: str,
        timeout_seconds: float = 15.0,
        max_concurrent_requests: int = 8,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests debe ser >= 1")
        self._store = store
        self._fetcher = fetcher
        self._rate_limiter = rate_limiter
        self._robots = robots
        self._policy = policy
        self._user_agent = user_agent
        self._timeout_seconds = timeout_seconds
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._clock = clock

    async def run(
        self,
        changed_pages_path: Path,
        removed_path: Path,
        *,
        max_pages: int | None = None,
    ) -> RecrawlReport:
        """Revisita como mucho `max_pages` páginas vencidas. Las cambiadas se
        añaden a `changed_pages_path` (formato `PageRecord`) y las
        desaparecidas a `removed_path` (`{"url": ..., "status_code": ...}`)."""
        due = self._store.due(self._clock(), max_pages)
        counters = _Counters()
        changed_pages_path.parent.mkdir(parents=True, exist_ok=True)
        removed_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            changed_pages_path.open("a", encoding="utf-8") as changed_file,
            removed_path.open("a", encoding="utf-8") as removed_file,
        ):
            results = await asyncio.gather(*(self._revisit(state) for state in due))
            for state, outcome in zip(due, results, strict=True):
                kind, payload = outcome
                if kind == "changed" and isinstance(payload, PageRecord):
                    changed_file.write(
                        json.dumps(payload.to_json_dict(), ensure_ascii=False) + "\n"
                    )
                    counters.changed += 1
                elif kind == "removed" and isinstance(payload, int):
                    removed_file.write(
                        json.dumps({"url": state.url, "status_code": payload}) + "\n"
                    )
                    counters.removed += 1
                elif kind == "not_modified":
                    counters.not_modified += 1
                elif kind == "unchanged_body":
                    counters.unchanged_body += 1
                elif kind == "robots_disallowed":
                    counters.robots_disallowed += 1
                else:
                    counters.failed += 1
        return RecrawlReport(checked=len(due), **_counters_dict(counters))

    async def _revisit(self, state: RecrawlState) -> tuple[str, PageRecord | int | None]:
        async with self._semaphore:
            min_delay: float | None = None
            if self._robots is not None:
                if not await self._robots.is_allowed(state.url, self._user_agent):
                    # Ya no se permite: se deja de re-rastrear, pero no se emite
                    # como borrada (sigue existiendo, solo no podemos visitarla).
                    self._store.remove(state.url)
                    return "robots_disallowed", None
                min_delay = await self._robots.crawl_delay(state.url, self._user_agent)

            await self._rate_limiter.acquire(state.url, min_delay)
            try:
                validators = None if state.validators.is_empty else state.validators
                result = await self._fetcher.fetch(state.url, self._timeout_seconds, validators)
            except FetchError as error:
                if error.status_code in _GONE_STATUSES:
                    self._store.remove(state.url)
                    return "removed", error.status_code
                logger.warning("re-rastreo fallido de %s: %s", state.url, error.reason)
                # Fallo transitorio: se reintenta tras el intervalo mínimo, sin
                # contar la visita en la estimación de la tasa de cambio.
                self._store.put(
                    replace(state, next_due_at=self._clock() + self._policy.min_interval)
                )
                return "failed", None
            finally:
                self._rate_limiter.release(state.url)

        now = self._clock()
        interval = max(0.0, (now - state.last_fetched_at).total_seconds())
        if result.status_code == HTTPStatus.NOT_MODIFIED:
            self._record_visit(state, now, interval, changed=False, result=result)
            return "not_modified", None

        digest = content_sha256(result.body)
        if digest == state.content_sha256:
            self._record_visit(state, now, interval, changed=False, result=result)
            return "unchanged_body", None

        self._record_visit(state, now, interval, changed=True, result=result, digest=digest)
        page = PageRecord(
            url=state.url,
            final_url=result.final_url,
            status_code=result.status_code,
            headers=result.headers,
            html=result.body,
            fetched_at=now,
            depth=state.depth,
            content_type=result.content_type,
        )
        return "changed", page

    def _record_visit(
        self,
        state: RecrawlState,
        now: datetime,
        interval: float,
        *,
        changed: bool,
        result: FetchResult,
        digest: str | None = None,
    ) -> None:
        checks = state.checks + 1
        changes = state.changes + (1 if changed else 0)
        total = state.total_interval_seconds + interval
        validators = CacheValidators.from_headers(result.headers)
        # Un 304 puede no repetir los validadores: se conservan los anteriores.
        etag = validators.etag if validators.etag is not None else state.etag
        last_modified = (
            validators.last_modified
            if validators.last_modified is not None
            else state.last_modified
        )
        previous_interval = (state.next_due_at - state.last_fetched_at).total_seconds()
        next_interval = self._policy.next_interval(checks, changes, total, previous_interval)
        self._store.put(
            replace(
                state,
                etag=etag,
                last_modified=last_modified,
                content_sha256=digest if digest is not None else state.content_sha256,
                last_fetched_at=now,
                next_due_at=now + next_interval,
                checks=checks,
                changes=changes,
                total_interval_seconds=total,
            )
        )


def _counters_dict(counters: _Counters) -> dict[str, int]:
    return {
        "not_modified": counters.not_modified,
        "unchanged_body": counters.unchanged_body,
        "changed": counters.changed,
        "removed": counters.removed,
        "failed": counters.failed,
        "robots_disallowed": counters.robots_disallowed,
    }
