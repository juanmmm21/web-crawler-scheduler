"""Normalización de URLs y deduplicación por hash normalizado.

Dos URLs que solo difieren en mayúsculas del host, un puerto por defecto
explícito, un fragmento (`#...`) o el orden de los parámetros de query
apuntan al mismo recurso — normalizarlas antes de hashear evita crawlear la
misma página varias veces por variaciones triviales de URL.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_url(url: str) -> str:
    """Normaliza una URL para deduplicación.

    Aplica: esquema y host en minúsculas, eliminación de puerto por defecto,
    eliminación de fragmento, orden estable de parámetros de query y
    eliminación de la barra final salvo en la raíz (`/`).
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    port = parts.port
    if port is not None and _DEFAULT_PORTS.get(scheme) == port:
        port = None
    netloc = hostname if port is None else f"{hostname}:{port}"
    if parts.username:
        userinfo = parts.username + (f":{parts.password}" if parts.password else "")
        netloc = f"{userinfo}@{netloc}"

    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/") or "/"

    query_pairs = sorted(parse_qsl(parts.query, keep_blank_values=True))
    query = urlencode(query_pairs)

    return urlunsplit((scheme, netloc, path, query, ""))


def url_hash(url: str) -> str:
    """Hash sha256 (hexdigest) de la URL normalizada: clave compacta de deduplicación."""
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def extract_domain(url: str) -> str:
    """Extrae `host[:puerto]` de una URL, usado como clave de rate limiting y caché de robots.txt.

    Se incluye el puerto (cuando no es el de defecto) porque dos servicios
    distintos en el mismo host pero puertos distintos no comparten ni
    política de robots.txt ni límites de *rate* razonables.
    """
    parts = urlsplit(url)
    hostname = (parts.hostname or "").lower()
    port = parts.port
    if port is not None and _DEFAULT_PORTS.get(parts.scheme.lower()) == port:
        port = None
    return hostname if port is None else f"{hostname}:{port}"


class HashSetDeduplicator:
    """Deduplicador en memoria basado en hash normalizado de URL.

    Implementa `protocols.Deduplicator` mediante *structural typing*: no
    hereda de él explícitamente, basta con exponer los mismos métodos.
    """

    def __init__(self, initial_hashes: Iterable[str] | None = None) -> None:
        self._seen_hashes: set[str] = set(initial_hashes) if initial_hashes is not None else set()

    def seen(self, url: str) -> bool:
        return url_hash(url) in self._seen_hashes

    def mark_seen(self, url: str) -> None:
        self._seen_hashes.add(url_hash(url))

    def snapshot(self) -> set[str]:
        """Copia del conjunto de hashes vistos, para volcar en un checkpoint."""
        return set(self._seen_hashes)
