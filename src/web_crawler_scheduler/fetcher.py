"""Descarga HTTP asíncrona con reintentos y backoff exponencial con jitter.

Los reintentos cubren errores transitorios —timeouts, errores de conexión y
respuestas 429/5xx—, donde reintentar tiene sentido porque el servidor puede
recuperarse. Un 4xx distinto de 429 es un error permanente del lado del
recurso (URL rota, acceso prohibido) y se descarta sin reintentar, para no
desperdiciar peticiones contra algo que nunca va a responder de otra forma.
"""

from __future__ import annotations

import asyncio
import random

import aiohttp

from web_crawler_scheduler.models import FetchResult

_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class FetchError(Exception):
    """Se agotaron los reintentos (o el error era permanente) al descargar una URL."""

    def __init__(self, url: str, reason: str, attempts: int) -> None:
        super().__init__(f"No se pudo descargar {url} tras {attempts} intento(s): {reason}")
        self.url = url
        self.reason = reason
        self.attempts = attempts


class AiohttpFetcher:
    """Descarga páginas vía aiohttp con reintentos y backoff exponencial con jitter."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        max_retries: int = 3,
        backoff_base_seconds: float = 1.0,
        backoff_max_seconds: float = 60.0,
        user_agent: str = "BeaconCrawler/0.1",
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries no puede ser negativo")
        if backoff_base_seconds <= 0:
            raise ValueError("backoff_base_seconds debe ser positivo")
        if backoff_max_seconds < backoff_base_seconds:
            raise ValueError("backoff_max_seconds no puede ser menor que backoff_base_seconds")
        self._session = session
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._backoff_max_seconds = backoff_max_seconds
        self._user_agent = user_agent

    async def fetch(self, url: str, timeout_seconds: float) -> FetchResult:
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        headers = {"User-Agent": self._user_agent}
        last_error = "razón desconocida"

        for attempt in range(self._max_retries + 1):
            is_last_attempt = attempt == self._max_retries
            try:
                async with self._session.get(url, timeout=timeout, headers=headers) as response:
                    if response.status in _RETRYABLE_STATUSES:
                        last_error = f"HTTP {response.status}"
                        if is_last_attempt:
                            raise FetchError(url, last_error, attempt + 1)
                        await self._sleep_backoff(attempt)
                        continue
                    if response.status >= 400:
                        raise FetchError(url, f"HTTP {response.status}", attempt + 1)
                    body = await response.text(errors="replace")
                    return FetchResult(
                        final_url=str(response.url),
                        status_code=response.status,
                        headers=dict(response.headers),
                        body=body,
                        content_type=response.content_type,
                    )
            except (aiohttp.ClientError, TimeoutError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if is_last_attempt:
                    raise FetchError(url, last_error, attempt + 1) from exc
                await self._sleep_backoff(attempt)

        # Inalcanzable en la práctica: `max_retries >= 0` garantiza al menos una
        # iteración, y esa última iteración siempre retorna o lanza. Se deja
        # como red de seguridad explícita para que mypy vea un cierre total de
        # la función y para no depender silenciosamente de ese invariante.
        raise FetchError(url, last_error, self._max_retries + 1)

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = min(self._backoff_base_seconds * (2**attempt), self._backoff_max_seconds)
        jitter = random.uniform(0, delay * 0.1)
        await asyncio.sleep(delay + jitter)
