"""Dobles de prueba para `aiohttp.ClientSession`, evitando red real y dependencias externas
frágiles ante cambios de versión de aiohttp (las librerías de mocking de aiohttp del
ecosistema quedan desactualizadas con frecuencia frente a sus versiones más recientes).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType


class FakeResponse:
    """Doble de prueba de `aiohttp.ClientResponse` con la superficie mínima que usa el código."""

    def __init__(
        self,
        status: int = 200,
        body: str = "",
        headers: Mapping[str, str] | None = None,
        content_type: str | None = "text/html",
        final_url: str | None = None,
        raise_on_enter: BaseException | None = None,
    ) -> None:
        self.status = status
        self.headers: dict[str, str] = dict(headers) if headers is not None else {}
        self.content_type = content_type
        self._body = body
        self._final_url = final_url
        self._raise_on_enter = raise_on_enter

    @property
    def url(self) -> str:
        return self._final_url if self._final_url is not None else ""

    async def text(self, errors: str = "strict") -> str:
        return self._body

    async def __aenter__(self) -> FakeResponse:
        if self._raise_on_enter is not None:
            raise self._raise_on_enter
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class FakeClientSession:
    """Doble de prueba de `aiohttp.ClientSession`: enruta `.get(url)` por URL exacta.

    Cada URL puede registrar una única respuesta (se reutiliza en todas las
    llamadas, como en los tests de caché) o una secuencia de respuestas que se
    van consumiendo en orden —imprescindible para simular reintentos, donde la
    misma URL responde distinto en cada intento— reutilizando la última una
    vez agotada la secuencia.
    """

    def __init__(
        self, responses_by_url: Mapping[str, FakeResponse | Sequence[FakeResponse]]
    ) -> None:
        self._queues: dict[str, list[FakeResponse]] = {
            url: list(response) if isinstance(response, Sequence) else [response]
            for url, response in responses_by_url.items()
        }
        self.requested_urls: list[str] = []
        self.requested_headers: list[Mapping[str, str] | None] = []

    def get(
        self, url: str, timeout: object = None, headers: Mapping[str, str] | None = None
    ) -> FakeResponse:
        self.requested_urls.append(url)
        self.requested_headers.append(headers)
        queue = self._queues.get(url)
        if not queue:
            raise AssertionError(f"No hay respuesta doble registrada para {url}")
        if len(queue) > 1:
            return queue.pop(0)
        return queue[0]
