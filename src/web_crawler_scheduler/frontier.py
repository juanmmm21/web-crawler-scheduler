"""Frontera de URLs pendientes de crawlear: cola de prioridad con desempate FIFO.

Por defecto crawlea en amplitud (BFS): la prioridad de una entrada es su
profundidad de descubrimiento (`bfs_priority`), así que las URLs semilla
(profundidad 0) se agotan antes que las descubiertas a partir de ellas. Puede
usarse una prioridad distinta asignándola explícitamente en
`FrontierEntry.priority` (p. ej. para priorizar por PageRank estimado).

El límite de profundidad máxima no se aplica aquí: es responsabilidad de
`pipeline.py`, que decide si una URL descubierta se encola o se descarta,
manteniendo esta cola como una estructura de datos genérica y reutilizable.
"""

from __future__ import annotations

import heapq
import itertools

from web_crawler_scheduler.models import FrontierEntry


class PriorityFrontier:
    """Cola de prioridad de `FrontierEntry`, con desempate FIFO por orden de inserción."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, FrontierEntry]] = []
        self._counter = itertools.count()

    def push(self, entry: FrontierEntry) -> None:
        heapq.heappush(self._heap, (entry.priority, next(self._counter), entry))

    def pop(self) -> FrontierEntry | None:
        if not self._heap:
            return None
        _, _, entry = heapq.heappop(self._heap)
        return entry

    def __len__(self) -> int:
        return len(self._heap)

    def to_entries(self) -> list[FrontierEntry]:
        """Snapshot de las entradas pendientes en orden de prioridad, para checkpoint."""
        return [entry for _, _, entry in sorted(self._heap)]

    @staticmethod
    def bfs_priority(depth: int) -> int:
        """Prioridad BFS estándar: la profundidad de descubrimiento de la URL."""
        return depth
