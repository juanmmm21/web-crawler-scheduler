"""Cumplimiento de `robots.txt`: descarga, parseo y caché por origen.

Sigue la convención estándar de los crawlers educados (la misma que documenta
Google para Googlebot): un `robots.txt` que responde con un error de cliente
(4xx) se interpreta como "sin restricciones" —el sitio no publica una
política válida—, mientras que un origen inalcanzable por error de servidor
(5xx), timeout o error de conexión se trata como "todo prohibido" de forma
temporal, porque no se puede confirmar qué permite el sitio y asumir permiso
sería arriesgarse a violar una política que sí existe pero no se pudo leer.
"""

from __future__ import annotations

import asyncio
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import aiohttp

from web_crawler_scheduler.urlnorm import extract_domain


def _robots_txt_url(url: str) -> str:
    """Deriva la URL de `robots.txt` para el origen (esquema+host+puerto) de `url`."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))


def _parse_crawl_delay(body: str, user_agent: str) -> float | None:
    """Extrae el valor de `Crawl-delay` del grupo que aplica a `user_agent`.

    `RobotFileParser` de la librería estándar solo admite valores enteros en
    `Crawl-delay` (usa `str.isdigit`), pero muchos sitios reales publican
    valores fraccionarios (p. ej. `Crawl-delay: 0.5`); por eso se parsea aquí
    de forma manual, respetando la semántica estándar de grupos: un grupo con
    user-agent exacto tiene prioridad sobre el grupo comodín `*`.

    El *matching* de user-agent replica el de `RobotFileParser.Entry.applies_to`:
    se compara el token de producto (antes de la primera `/`) del user-agent
    del crawler contra cada token declarado en robots.txt, por subcadena.
    """
    target = user_agent.split("/")[0].strip().lower()
    groups: list[tuple[list[str], float | None]] = []
    current_agents: list[str] = []
    current_delay: float | None = None
    group_has_rule = False

    def close_group() -> None:
        nonlocal current_agents, current_delay, group_has_rule
        if current_agents:
            groups.append((current_agents, current_delay))
        current_agents = []
        current_delay = None
        group_has_rule = False

    for raw_line in body.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip().lower()
        value = value.strip()
        if key == "user-agent":
            if group_has_rule:
                close_group()
            current_agents.append(value.lower())
        elif key == "crawl-delay":
            try:
                current_delay = float(value)
            except ValueError:
                current_delay = None
            group_has_rule = True
        elif key in ("allow", "disallow", "request-rate"):
            group_has_rule = True
    close_group()

    wildcard_delay: float | None = None
    for agents, delay in groups:
        if delay is None:
            continue
        if any(agent != "*" and agent in target for agent in agents):
            return delay
        if "*" in agents and wildcard_delay is None:
            wildcard_delay = delay
    return wildcard_delay


class _OriginPolicy:
    """Política de robots.txt ya resuelta y cacheada para un origen concreto."""

    __slots__ = ("_body", "_parser", "_reachable")

    def __init__(self, parser: RobotFileParser | None, reachable: bool, body: str | None) -> None:
        self._parser = parser
        self._reachable = reachable
        self._body = body

    def is_allowed(self, url: str, user_agent: str) -> bool:
        if not self._reachable:
            return False
        if self._parser is None:
            return True
        return self._parser.can_fetch(user_agent, url)

    def crawl_delay(self, user_agent: str) -> float | None:
        if not self._reachable or self._body is None:
            return None
        return _parse_crawl_delay(self._body, user_agent)


class RobotsCache:
    """Descarga, parsea y cachea `robots.txt` por origen durante toda la vida del crawl.

    Una única instancia debe compartirse entre todas las peticiones de un
    mismo crawl para no repetir la descarga de `robots.txt` en cada URL del
    mismo origen; el acceso concurrente al mismo origen se serializa con un
    `asyncio.Lock` por origen para evitar descargarlo dos veces en paralelo.
    """

    def __init__(self, session: aiohttp.ClientSession, timeout_seconds: float = 10.0) -> None:
        self._session = session
        self._timeout_seconds = timeout_seconds
        self._policies: dict[str, _OriginPolicy] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def _get_policy(self, url: str) -> _OriginPolicy:
        origin = extract_domain(url)
        cached = self._policies.get(origin)
        if cached is not None:
            return cached

        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:
            cached = self._policies.get(origin)
            if cached is not None:
                return cached
            policy = await self._fetch_policy(url)
            self._policies[origin] = policy
            return policy

    async def _fetch_policy(self, url: str) -> _OriginPolicy:
        robots_url = _robots_txt_url(url)
        timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
        try:
            async with self._session.get(robots_url, timeout=timeout) as response:
                if 200 <= response.status < 300:
                    body = await response.text(errors="replace")
                elif 400 <= response.status < 500:
                    return _OriginPolicy(parser=None, reachable=True, body=None)
                else:
                    return _OriginPolicy(parser=None, reachable=False, body=None)
        except (aiohttp.ClientError, TimeoutError):
            return _OriginPolicy(parser=None, reachable=False, body=None)

        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(body.splitlines())
        return _OriginPolicy(parser=parser, reachable=True, body=body)

    async def is_allowed(self, url: str, user_agent: str) -> bool:
        policy = await self._get_policy(url)
        return policy.is_allowed(url, user_agent)

    async def crawl_delay(self, url: str, user_agent: str) -> float | None:
        policy = await self._get_policy(url)
        return policy.crawl_delay(user_agent)
