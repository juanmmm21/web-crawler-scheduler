"""Tipos de datos del dominio para web-crawler-scheduler.

Todas las estructuras son inmutables (frozen dataclasses) salvo el estado de
checkpoint, cuyo propósito es precisamente mutar y serializarse durante el
crawl para permitir reanudar sin perder la frontera.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class FetchOutcome(StrEnum):
    """Resultado final de un intento de descarga de una URL."""

    SUCCESS = "success"
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    ROBOTS_DISALLOWED = "robots_disallowed"
    DISCARDED_AFTER_RETRIES = "discarded_after_retries"


@dataclass(frozen=True, slots=True)
class CrawlConfig:
    """Configuración completa de una ejecución de crawl.

    Se valida en `__post_init__` porque un config inválido (p. ej. cero
    peticiones concurrentes) debe fallar de forma explícita al construirse,
    no silenciosamente a mitad de crawl.
    """

    seed_urls: tuple[str, ...]
    max_pages: int = 1000
    max_depth: int = 3
    max_concurrent_requests: int = 50
    max_concurrent_per_domain: int = 2
    default_min_delay_seconds: float = 1.0
    request_timeout_seconds: float = 15.0
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    user_agent: str = "BeaconCrawler/0.1 (+https://github.com/juanmmm21/web-crawler-scheduler)"
    obey_robots_txt: bool = True

    def __post_init__(self) -> None:
        if not self.seed_urls:
            raise ValueError("CrawlConfig requiere al menos una URL semilla")
        if self.max_pages <= 0:
            raise ValueError("max_pages debe ser positivo")
        if self.max_depth < 0:
            raise ValueError("max_depth no puede ser negativo")
        if self.max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests debe ser positivo")
        if self.max_concurrent_per_domain <= 0:
            raise ValueError("max_concurrent_per_domain debe ser positivo")
        if self.default_min_delay_seconds < 0:
            raise ValueError("default_min_delay_seconds no puede ser negativo")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds debe ser positivo")
        if self.max_retries < 0:
            raise ValueError("max_retries no puede ser negativo")
        if self.backoff_base_seconds <= 0:
            raise ValueError("backoff_base_seconds debe ser positivo")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds no puede ser menor que backoff_base_seconds")


@dataclass(frozen=True, slots=True)
class PageRecord:
    """Una página crawleada con éxito, lista para serializar en JSONL.

    `url` conserva la URL original de la frontera y `final_url` la URL tras
    seguir redirecciones — ambas se guardan porque el extractor y el grafo de
    enlaces necesitan poder correlacionar una u otra según el consumidor.
    """

    url: str
    final_url: str
    status_code: int
    headers: dict[str, str]
    html: str
    fetched_at: datetime
    depth: int
    content_type: str | None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "final_url": self.final_url,
            "status_code": self.status_code,
            "headers": self.headers,
            "html": self.html,
            "fetched_at": self.fetched_at.isoformat(),
            "depth": self.depth,
            "content_type": self.content_type,
        }

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> PageRecord:
        return PageRecord(
            url=str(data["url"]),
            final_url=str(data["final_url"]),
            status_code=int(data["status_code"]),
            headers={str(k): str(v) for k, v in data["headers"].items()},
            html=str(data["html"]),
            fetched_at=datetime.fromisoformat(str(data["fetched_at"])),
            depth=int(data["depth"]),
            content_type=None if data["content_type"] is None else str(data["content_type"]),
        )


@dataclass(frozen=True, slots=True)
class LinkGraphEntry:
    """Enlaces salientes descubiertos en una página, consumidos por pagerank-link-analysis."""

    url: str
    outlinks: tuple[str, ...]

    def to_json_dict(self) -> dict[str, Any]:
        return {"url": self.url, "outlinks": list(self.outlinks)}

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> LinkGraphEntry:
        return LinkGraphEntry(
            url=str(data["url"]),
            outlinks=tuple(str(u) for u in data["outlinks"]),
        )


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Resultado crudo de una petición HTTP exitosa, previo a convertirse en `PageRecord`.

    Se mantiene separado de `PageRecord` porque el fetcher no conoce la
    profundidad de la URL en la frontera; esa información la añade el pipeline
    al ensamblar el `PageRecord` final.
    """

    final_url: str
    status_code: int
    headers: dict[str, str]
    body: str
    content_type: str | None


@dataclass(frozen=True, slots=True)
class FrontierEntry:
    """Una URL pendiente de crawlear, con su prioridad y profundidad de descubrimiento."""

    url: str
    depth: int
    priority: int
    discovered_from: str | None = None


@dataclass(frozen=True, slots=True)
class DiscardedUrl:
    """Una URL descartada definitivamente (robots, agotar reintentos, etc.), para auditoría."""

    url: str
    reason: str
    outcome: FetchOutcome
    attempts: int
    discarded_at: datetime

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "reason": self.reason,
            "outcome": self.outcome.value,
            "attempts": self.attempts,
            "discarded_at": self.discarded_at.isoformat(),
        }


@dataclass(slots=True)
class CheckpointState:
    """Estado serializable de un crawl interrumpido, para reanudarlo sin perder la frontera.

    Es la única estructura mutable del módulo: se actualiza en vivo durante el
    crawl y se vuelca a disco periódicamente para poder reanudar tras un fallo.

    No conserva los timestamps de última petición por dominio del rate
    limiter: si el proceso estuvo detenido, cualquier demora mínima razonable
    (segundos) ya ha transcurrido de sobra en tiempo real, así que reanudar
    con el rate limiter "en frío" es correcto y evita serializar estado cuyo
    origen temporal (`time.monotonic()`) no sobrevive a un reinicio del proceso.
    """

    visited_hashes: set[str] = field(default_factory=set)
    frontier_entries: list[FrontierEntry] = field(default_factory=list)
    pages_crawled: int = 0

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "visited_hashes": sorted(self.visited_hashes),
            "frontier_entries": [
                {
                    "url": e.url,
                    "depth": e.depth,
                    "priority": e.priority,
                    "discovered_from": e.discovered_from,
                }
                for e in self.frontier_entries
            ],
            "pages_crawled": self.pages_crawled,
        }

    @staticmethod
    def from_json_dict(data: dict[str, Any]) -> CheckpointState:
        return CheckpointState(
            visited_hashes=set(data["visited_hashes"]),
            frontier_entries=[
                FrontierEntry(
                    url=str(e["url"]),
                    depth=int(e["depth"]),
                    priority=int(e["priority"]),
                    discovered_from=e["discovered_from"],
                )
                for e in data["frontier_entries"]
            ],
            pages_crawled=int(data["pages_crawled"]),
        )
