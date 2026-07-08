"""Rate limiting por dominio: concurrencia máxima simultánea y demora mínima entre peticiones.

Cada dominio obtiene su propio semáforo (concurrencia) y su propio timestamp
de última petición (demora mínima), creados de forma perezosa la primera vez
que se ve ese dominio — el crawler no conoce de antemano qué dominios va a
visitar.
"""

from __future__ import annotations

import asyncio
import time

from web_crawler_scheduler.urlnorm import extract_domain


class DomainRateLimiter:
    """Limita peticiones concurrentes y aplica demora mínima entre peticiones, por dominio."""

    def __init__(self, max_concurrent_per_domain: int, default_min_delay_seconds: float) -> None:
        if max_concurrent_per_domain <= 0:
            raise ValueError("max_concurrent_per_domain debe ser positivo")
        if default_min_delay_seconds < 0:
            raise ValueError("default_min_delay_seconds no puede ser negativo")
        self._max_concurrent_per_domain = max_concurrent_per_domain
        self._default_min_delay_seconds = default_min_delay_seconds
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._last_request_monotonic: dict[str, float] = {}

    def _semaphore_for(self, domain: str) -> asyncio.Semaphore:
        semaphore = self._semaphores.get(domain)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._max_concurrent_per_domain)
            self._semaphores[domain] = semaphore
        return semaphore

    def _lock_for(self, domain: str) -> asyncio.Lock:
        lock = self._domain_locks.get(domain)
        if lock is None:
            lock = asyncio.Lock()
            self._domain_locks[domain] = lock
        return lock

    async def acquire(self, url: str, min_delay_seconds: float | None = None) -> None:
        """Bloquea hasta que hay hueco de concurrencia y ha pasado la demora mínima del dominio.

        `min_delay_seconds` permite pasar el `Crawl-delay` específico de
        robots.txt para ese dominio; si es `None` se usa el valor por defecto
        de la instancia.
        """
        delay = self._default_min_delay_seconds if min_delay_seconds is None else min_delay_seconds
        if delay < 0:
            raise ValueError("min_delay_seconds no puede ser negativo")

        domain = extract_domain(url)
        await self._semaphore_for(domain).acquire()

        # El lock serializa lectura+escritura del timestamp: sin él, dos
        # tareas concurrentes podrían leer el mismo `last_request` y
        # despertarse a la vez, violando la demora mínima entre peticiones.
        async with self._lock_for(domain):
            last_request = self._last_request_monotonic.get(domain)
            now = time.monotonic()
            if last_request is not None:
                remaining = delay - (now - last_request)
                if remaining > 0:
                    await asyncio.sleep(remaining)
            self._last_request_monotonic[domain] = time.monotonic()

    def release(self, url: str) -> None:
        """Libera el hueco de concurrencia adquirido por `acquire()` para el dominio de `url`."""
        domain = extract_domain(url)
        semaphore = self._semaphores.get(domain)
        if semaphore is None:
            raise RuntimeError(
                f"release() llamado para un dominio sin acquire() previo: {domain!r}"
            )
        semaphore.release()
