"""CLI de `web-crawler-scheduler`: `crawl` (con reanudación), `stats` sobre un
`pages.jsonl` y `recrawl` (revisita según la tasa de cambio de cada página)."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import aiohttp

from web_crawler_scheduler.fetcher import AiohttpFetcher
from web_crawler_scheduler.models import CrawlConfig, CrawlStats
from web_crawler_scheduler.pipeline import CrawlPipeline
from web_crawler_scheduler.rate_limiter import DomainRateLimiter
from web_crawler_scheduler.recrawl import ChangeRatePolicy, RecrawlPipeline, RecrawlStore
from web_crawler_scheduler.robots import RobotsCache

logger = logging.getLogger("web_crawler_scheduler")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="web-crawler-scheduler",
        description="Crawler asíncrono y educado con frontera priorizada y grafo de enlaces.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl_parser = subparsers.add_parser("crawl", help="Ejecuta (o reanuda) un crawl")
    crawl_parser.add_argument(
        "--seed", action="append", dest="seeds", default=[], help="URL semilla (repetible)"
    )
    crawl_parser.add_argument(
        "--seeds-file",
        type=Path,
        default=None,
        help="Fichero con una URL semilla por línea, además de las pasadas con --seed",
    )
    crawl_parser.add_argument(
        "--output-dir", type=Path, required=True, help="Directorio de salida (se crea si no existe)"
    )
    crawl_parser.add_argument("--max-pages", type=int, default=1000)
    crawl_parser.add_argument("--max-depth", type=int, default=3)
    crawl_parser.add_argument("--max-concurrent-requests", type=int, default=50)
    crawl_parser.add_argument("--max-concurrent-per-domain", type=int, default=2)
    crawl_parser.add_argument("--min-delay-seconds", type=float, default=1.0)
    crawl_parser.add_argument("--timeout-seconds", type=float, default=15.0)
    crawl_parser.add_argument("--max-retries", type=int, default=3)
    crawl_parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    crawl_parser.add_argument("--backoff-max-seconds", type=float, default=60.0)
    crawl_parser.add_argument(
        "--user-agent",
        default="BeaconCrawler/0.1 (+https://github.com/juanmmm21/web-crawler-scheduler)",
    )
    crawl_parser.add_argument(
        "--ignore-robots", action="store_true", help="No respetar robots.txt (usar con cautela)"
    )
    crawl_parser.add_argument(
        "--allow-prefix",
        action="append",
        dest="allowed_prefixes",
        default=[],
        help="Solo sigue enlaces que empiezan por este prefijo de URL (repetible)",
    )
    crawl_parser.add_argument("--checkpoint-every", type=int, default=50)
    crawl_parser.add_argument(
        "--resume",
        action="store_true",
        help="Reanuda desde el checkpoint si existe en --output-dir",
    )

    stats_parser = subparsers.add_parser("stats", help="Muestra estadísticas de un pages.jsonl")
    stats_parser.add_argument("pages_path", type=Path)

    recrawl_parser = subparsers.add_parser(
        "recrawl", help="Revisita las páginas vencidas según su tasa de cambio"
    )
    recrawl_parser.add_argument("--state", type=Path, required=True, help="recrawl_state.json")
    recrawl_parser.add_argument(
        "--seed-pages", type=Path, default=None, help="pages.jsonl de un crawl previo a registrar"
    )
    recrawl_parser.add_argument("--output-dir", type=Path, required=True)
    recrawl_parser.add_argument("--max-pages", type=int, default=None)
    recrawl_parser.add_argument("--max-concurrent-per-domain", type=int, default=2)
    recrawl_parser.add_argument("--min-delay-seconds", type=float, default=1.0)
    recrawl_parser.add_argument("--timeout-seconds", type=float, default=15.0)
    recrawl_parser.add_argument("--initial-interval-hours", type=float, default=24.0)
    recrawl_parser.add_argument("--min-interval-hours", type=float, default=1.0)
    recrawl_parser.add_argument("--max-interval-days", type=float, default=30.0)
    recrawl_parser.add_argument(
        "--user-agent",
        default="BeaconCrawler/0.1 (+https://github.com/juanmmm21/web-crawler-scheduler)",
    )

    return parser


async def _recrawl(args: argparse.Namespace, store: RecrawlStore, policy: ChangeRatePolicy) -> str:
    async with aiohttp.ClientSession() as session:
        pipeline = RecrawlPipeline(
            store,
            AiohttpFetcher(session, user_agent=args.user_agent),
            DomainRateLimiter(
                max_concurrent_per_domain=args.max_concurrent_per_domain,
                default_min_delay_seconds=args.min_delay_seconds,
            ),
            RobotsCache(session, timeout_seconds=args.timeout_seconds),
            policy=policy,
            user_agent=args.user_agent,
            timeout_seconds=args.timeout_seconds,
        )
        report = await pipeline.run(
            args.output_dir / "pages.jsonl",
            args.output_dir / "removed.jsonl",
            max_pages=args.max_pages,
        )
    return (
        f"Revisitadas {report.checked}: {report.changed} cambiadas, "
        f"{report.not_modified} sin cambios (304), {report.unchanged_body} sin cambios (hash), "
        f"{report.removed} desaparecidas, {report.failed} fallidas, "
        f"{report.robots_disallowed} bloqueadas por robots.txt"
    )


def _run_recrawl(args: argparse.Namespace) -> int:
    try:
        policy = ChangeRatePolicy(
            initial_interval=timedelta(hours=args.initial_interval_hours),
            min_interval=timedelta(hours=args.min_interval_hours),
            max_interval=timedelta(days=args.max_interval_days),
        )
        store = RecrawlStore.load(args.state) if args.state.exists() else RecrawlStore()
    except ValueError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2
    if args.seed_pages is not None:
        if not args.seed_pages.exists():
            print(f"Error: no existe {args.seed_pages}", file=sys.stderr)
            return 2
        print(f"Páginas nuevas registradas: {store.seed_from_pages(args.seed_pages, policy)}")
    try:
        print(asyncio.run(_recrawl(args, store, policy)))
    finally:
        # El estado se guarda también si el re-rastreo se interrumpe: las
        # visitas ya hechas no se repiten en la siguiente ejecución.
        store.save(args.state)
    return 0


def _load_seed_urls(seeds: list[str], seeds_file: Path | None) -> tuple[str, ...]:
    urls = list(seeds)
    if seeds_file is not None:
        urls.extend(
            line.strip()
            for line in seeds_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return tuple(urls)


async def _crawl(
    config: CrawlConfig,
    pages_path: Path,
    link_graph_path: Path,
    discarded_path: Path,
    checkpoint_path: Path,
    checkpoint_every: int,
    resume: bool,
) -> CrawlStats:
    async with aiohttp.ClientSession() as session:
        pipeline = (
            CrawlPipeline.resume_from_checkpoint(
                config,
                session,
                pages_path,
                link_graph_path,
                discarded_path,
                checkpoint_path=checkpoint_path,
                checkpoint_every=checkpoint_every,
            )
            if resume
            else CrawlPipeline(
                config,
                session,
                pages_path,
                link_graph_path,
                discarded_path,
                checkpoint_path=checkpoint_path,
                checkpoint_every=checkpoint_every,
            )
        )
        async with pipeline as active_pipeline:
            return await active_pipeline.run()


def _run_crawl(args: argparse.Namespace) -> int:
    seed_urls = _load_seed_urls(args.seeds, args.seeds_file)
    if not seed_urls:
        print(
            "Error: se requiere al menos una URL semilla (--seed o --seeds-file)", file=sys.stderr
        )
        return 2

    config = CrawlConfig(
        seed_urls=seed_urls,
        max_pages=args.max_pages,
        max_depth=args.max_depth,
        max_concurrent_requests=args.max_concurrent_requests,
        max_concurrent_per_domain=args.max_concurrent_per_domain,
        default_min_delay_seconds=args.min_delay_seconds,
        request_timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        backoff_base_seconds=args.backoff_base_seconds,
        backoff_max_seconds=args.backoff_max_seconds,
        user_agent=args.user_agent,
        obey_robots_txt=not args.ignore_robots,
        allowed_url_prefixes=tuple(args.allowed_prefixes),
    )

    output_dir: Path = args.output_dir
    checkpoint_path = output_dir / "checkpoint.json"
    resume = bool(args.resume) and checkpoint_path.exists()
    if args.resume and not checkpoint_path.exists():
        logger.warning("No existe checkpoint en %s; se arranca un crawl nuevo", checkpoint_path)

    stats = asyncio.run(
        _crawl(
            config,
            output_dir / "pages.jsonl",
            output_dir / "link_graph.jsonl",
            output_dir / "discarded.jsonl",
            checkpoint_path,
            args.checkpoint_every,
            resume=resume,
        )
    )
    print(f"Crawl finalizado: {stats.pages_crawled} páginas, {stats.urls_discarded} descartadas")
    return 0


def _run_stats(args: argparse.Namespace) -> int:
    pages_path: Path = args.pages_path
    if not pages_path.exists():
        print(f"Error: no existe {pages_path}", file=sys.stderr)
        return 2

    total = 0
    status_counts: dict[int, int] = {}
    total_html_bytes = 0
    for line in pages_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        total += 1
        status = int(record["status_code"])
        status_counts[status] = status_counts.get(status, 0) + 1
        total_html_bytes += len(str(record["html"]).encode("utf-8"))

    print(f"Páginas: {total}")
    for status in sorted(status_counts):
        print(f"  HTTP {status}: {status_counts[status]}")
    if total > 0:
        print(f"Tamaño medio de HTML: {total_html_bytes / total:.0f} bytes")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "crawl":
        return _run_crawl(args)
    if args.command == "stats":
        return _run_stats(args)
    if args.command == "recrawl":
        return _run_recrawl(args)

    parser.error(f"Comando desconocido: {args.command}")
    return 2  # inalcanzable: parser.error() termina el proceso con sys.exit()


if __name__ == "__main__":
    sys.exit(main())
