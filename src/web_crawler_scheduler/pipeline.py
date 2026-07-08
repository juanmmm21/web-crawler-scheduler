"""Orquestador del crawl: conecta frontera, robots, rate limiter, fetcher y extracción de
enlaces, y persiste resultados en JSONL con soporte de checkpoint y reanudación.

Concurrencia: en cada vuelta del bucle principal se lanzan tareas asíncronas
hasta el límite `max_concurrent_requests` de `CrawlConfig`; el límite por
dominio lo impone `DomainRateLimiter` de forma independiente. Dos tareas
concurrentes pueden descubrir la misma URL nueva antes de que ninguna de las
dos la haya procesado todavía (el marcado de "visto" ocurre al *procesar* una
entrada de la frontera, no al encolarla) — esto puede producir como mucho una
entrada duplicada en la frontera, nunca una descarga duplicada, porque la
comprobación de deduplicación al inicio de `_process_entry` descarta la
segunda ocurrencia antes de llegar a la red.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

import aiohttp

from web_crawler_scheduler.fetcher import AiohttpFetcher, FetchError
from web_crawler_scheduler.frontier import PriorityFrontier
from web_crawler_scheduler.link_extractor import extract_outlinks
from web_crawler_scheduler.models import (
    CheckpointState,
    CrawlConfig,
    CrawlStats,
    DiscardedUrl,
    FetchOutcome,
    FrontierEntry,
    LinkGraphEntry,
    PageRecord,
)
from web_crawler_scheduler.rate_limiter import DomainRateLimiter
from web_crawler_scheduler.robots import RobotsCache
from web_crawler_scheduler.urlnorm import HashSetDeduplicator, normalize_url

logger = logging.getLogger(__name__)


class CrawlPipeline:
    """Ejecuta un crawl completo: frontera -> robots -> rate limit -> fetch -> extracción.

    Se usa como *async context manager* para garantizar que los ficheros de
    salida se cierran explícitamente incluso si el crawl se interrumpe:

        async with CrawlPipeline(config, session, ...) as pipeline:
            stats = await pipeline.run()
    """

    def __init__(
        self,
        config: CrawlConfig,
        session: aiohttp.ClientSession,
        pages_output_path: Path,
        link_graph_output_path: Path,
        discarded_output_path: Path,
        checkpoint_path: Path | None = None,
        checkpoint_every: int = 50,
    ) -> None:
        if checkpoint_every <= 0:
            raise ValueError("checkpoint_every debe ser positivo")
        self._config = config
        self._pages_output_path = pages_output_path
        self._link_graph_output_path = link_graph_output_path
        self._discarded_output_path = discarded_output_path
        self._checkpoint_path = checkpoint_path
        self._checkpoint_every = checkpoint_every

        self._frontier = PriorityFrontier()
        self._dedup = HashSetDeduplicator()
        self._robots = RobotsCache(session, timeout_seconds=config.request_timeout_seconds)
        self._rate_limiter = DomainRateLimiter(
            max_concurrent_per_domain=config.max_concurrent_per_domain,
            default_min_delay_seconds=config.default_min_delay_seconds,
        )
        self._fetcher = AiohttpFetcher(
            session,
            max_retries=config.max_retries,
            backoff_base_seconds=config.backoff_base_seconds,
            backoff_max_seconds=config.backoff_max_seconds,
            user_agent=config.user_agent,
        )

        self._pages_crawled = 0
        self._urls_discarded = 0
        self._pages_file: TextIO | None = None
        self._link_graph_file: TextIO | None = None
        self._discarded_file: TextIO | None = None

    @classmethod
    def resume_from_checkpoint(
        cls,
        config: CrawlConfig,
        session: aiohttp.ClientSession,
        pages_output_path: Path,
        link_graph_output_path: Path,
        discarded_output_path: Path,
        checkpoint_path: Path,
        checkpoint_every: int = 50,
    ) -> CrawlPipeline:
        """Reconstruye un pipeline a partir de un checkpoint, sin perder la frontera pendiente."""
        pipeline = cls(
            config=config,
            session=session,
            pages_output_path=pages_output_path,
            link_graph_output_path=link_graph_output_path,
            discarded_output_path=discarded_output_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=checkpoint_every,
        )
        state = CheckpointState.from_json_dict(json.loads(checkpoint_path.read_text("utf-8")))
        pipeline._dedup = HashSetDeduplicator(initial_hashes=state.visited_hashes)
        for entry in state.frontier_entries:
            pipeline._frontier.push(entry)
        pipeline._pages_crawled = state.pages_crawled
        return pipeline

    async def __aenter__(self) -> CrawlPipeline:
        self._pages_output_path.parent.mkdir(parents=True, exist_ok=True)
        self._link_graph_output_path.parent.mkdir(parents=True, exist_ok=True)
        self._discarded_output_path.parent.mkdir(parents=True, exist_ok=True)
        self._pages_file = self._pages_output_path.open("a", encoding="utf-8")
        self._link_graph_file = self._link_graph_output_path.open("a", encoding="utf-8")
        self._discarded_file = self._discarded_output_path.open("a", encoding="utf-8")
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        for handle in (self._pages_file, self._link_graph_file, self._discarded_file):
            if handle is not None:
                handle.close()
        self._pages_file = None
        self._link_graph_file = None
        self._discarded_file = None

    async def run(self) -> CrawlStats:
        """Ejecuta el crawl hasta agotar la frontera o alcanzar `max_pages`."""
        if self._pages_crawled == 0 and len(self._frontier) == 0:
            self._seed_frontier()

        pending_tasks: set[asyncio.Task[None]] = set()
        while True:
            while (
                len(pending_tasks) < self._config.max_concurrent_requests
                and self._pages_crawled < self._config.max_pages
            ):
                entry = self._frontier.pop()
                if entry is None:
                    break
                pending_tasks.add(asyncio.create_task(self._process_entry(entry)))

            if not pending_tasks:
                break

            done, pending_tasks = await asyncio.wait(
                pending_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()  # relanza cualquier excepción no controlada del worker

            should_checkpoint = (
                self._checkpoint_path is not None
                and self._pages_crawled % self._checkpoint_every == 0
            )
            if should_checkpoint:
                self._save_checkpoint()

        if self._checkpoint_path is not None:
            self._save_checkpoint()

        return CrawlStats(pages_crawled=self._pages_crawled, urls_discarded=self._urls_discarded)

    def _seed_frontier(self) -> None:
        for seed_url in self._config.seed_urls:
            self._frontier.push(
                FrontierEntry(
                    url=seed_url,
                    depth=0,
                    priority=PriorityFrontier.bfs_priority(0),
                    discovered_from=None,
                )
            )

    async def _process_entry(self, entry: FrontierEntry) -> None:
        url = entry.url
        if self._dedup.seen(url):
            return
        self._dedup.mark_seen(url)

        min_delay: float | None = None
        if self._config.obey_robots_txt:
            allowed = await self._robots.is_allowed(url, self._config.user_agent)
            if not allowed:
                self._write_discarded(
                    url, "bloqueada por robots.txt", FetchOutcome.ROBOTS_DISALLOWED, attempts=0
                )
                return
            min_delay = await self._robots.crawl_delay(url, self._config.user_agent)

        await self._rate_limiter.acquire(url, min_delay)
        try:
            result = await self._fetcher.fetch(url, self._config.request_timeout_seconds)
        except FetchError as exc:
            self._write_discarded(
                url, exc.reason, FetchOutcome.DISCARDED_AFTER_RETRIES, exc.attempts
            )
            return
        finally:
            self._rate_limiter.release(url)

        page = PageRecord(
            url=url,
            final_url=result.final_url,
            status_code=result.status_code,
            headers=result.headers,
            html=result.body,
            fetched_at=datetime.now(UTC),
            depth=entry.depth,
            content_type=result.content_type,
        )
        self._write_page(page)
        self._pages_crawled += 1

        outlinks = self._extract_outlinks_safely(page)
        self._write_link_graph(LinkGraphEntry(url=page.final_url, outlinks=tuple(outlinks)))

        if entry.depth < self._config.max_depth:
            self._enqueue_outlinks(outlinks, parent_url=url, child_depth=entry.depth + 1)

    def _extract_outlinks_safely(self, page: PageRecord) -> list[str]:
        try:
            return extract_outlinks(page.html, page.final_url)
        except Exception:  # noqa: BLE001 - aísla un fallo de parseo de una página del resto del crawl
            logger.warning("No se pudieron extraer enlaces de %s", page.final_url, exc_info=True)
            return []

    def _enqueue_outlinks(self, outlinks: list[str], parent_url: str, child_depth: int) -> None:
        for outlink in outlinks:
            normalized = normalize_url(outlink)
            if self._dedup.seen(normalized):
                continue
            self._frontier.push(
                FrontierEntry(
                    url=outlink,
                    depth=child_depth,
                    priority=PriorityFrontier.bfs_priority(child_depth),
                    discovered_from=parent_url,
                )
            )

    def _write_page(self, page: PageRecord) -> None:
        assert self._pages_file is not None, "usar CrawlPipeline dentro de 'async with'"
        self._pages_file.write(json.dumps(page.to_json_dict(), ensure_ascii=False) + "\n")
        self._pages_file.flush()

    def _write_link_graph(self, entry: LinkGraphEntry) -> None:
        assert self._link_graph_file is not None, "usar CrawlPipeline dentro de 'async with'"
        self._link_graph_file.write(json.dumps(entry.to_json_dict(), ensure_ascii=False) + "\n")
        self._link_graph_file.flush()

    def _write_discarded(
        self, url: str, reason: str, outcome: FetchOutcome, attempts: int
    ) -> None:
        assert self._discarded_file is not None, "usar CrawlPipeline dentro de 'async with'"
        discarded = DiscardedUrl(
            url=url,
            reason=reason,
            outcome=outcome,
            attempts=attempts,
            discarded_at=datetime.now(UTC),
        )
        self._discarded_file.write(json.dumps(discarded.to_json_dict(), ensure_ascii=False) + "\n")
        self._discarded_file.flush()
        self._urls_discarded += 1

    def _save_checkpoint(self) -> None:
        if self._checkpoint_path is None:
            return
        state = CheckpointState(
            visited_hashes=self._dedup.snapshot(),
            frontier_entries=self._frontier.to_entries(),
            pages_crawled=self._pages_crawled,
        )
        tmp_path = self._checkpoint_path.with_name(self._checkpoint_path.name + ".tmp")
        tmp_path.write_text(json.dumps(state.to_json_dict()), encoding="utf-8")
        tmp_path.replace(self._checkpoint_path)
