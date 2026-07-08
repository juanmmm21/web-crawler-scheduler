"""Interfaces abstractas que desacoplan `pipeline.py` de sus implementaciones concretas.

Definir estos protocolos antes que la lógica permite testear el pipeline con
dobles de prueba (fakes en memoria) sin red real ni un event loop de aiohttp,
y permite sustituir cualquier pieza (p. ej. el fetcher) sin tocar el resto.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from web_crawler_scheduler.models import FetchResult, FrontierEntry


@runtime_checkable
class RobotsPolicy(Protocol):
    """Resuelve si una URL puede crawlearse y con qué demora mínima, según robots.txt."""

    async def is_allowed(self, url: str, user_agent: str) -> bool: ...

    async def crawl_delay(self, url: str, user_agent: str) -> float | None: ...


@runtime_checkable
class Frontier(Protocol):
    """Cola de prioridad de URLs pendientes de crawlear (BFS por defecto)."""

    def push(self, entry: FrontierEntry) -> None: ...

    def pop(self) -> FrontierEntry | None: ...

    def __len__(self) -> int: ...


@runtime_checkable
class Deduplicator(Protocol):
    """Detecta URLs ya vistas mediante hash normalizado, evitando reencolarlas."""

    def seen(self, url: str) -> bool: ...

    def mark_seen(self, url: str) -> None: ...


@runtime_checkable
class RateLimiter(Protocol):
    """Aplica límites de concurrencia máxima y demora mínima entre peticiones por dominio."""

    async def acquire(self, url: str, min_delay_seconds: float) -> None: ...

    def release(self, url: str) -> None: ...


@runtime_checkable
class PageFetcher(Protocol):
    """Descarga el contenido crudo de una URL; la política de reintentos es interna."""

    async def fetch(self, url: str, timeout_seconds: float) -> FetchResult: ...
