# web-crawler-scheduler

**Project 1/10 of the [`beacon-search-engine`](https://github.com/juanmmm21/beacon-search-engine) ecosystem** — *Ingestion* category.
Repository: [`github.com/juanmmm21/web-crawler-scheduler`](https://github.com/juanmmm21/web-crawler-scheduler)

An asynchronous, polite web crawler: it walks the web starting from a set of
seed URLs, respects `robots.txt` and per-domain rate limits, maintains a
prioritized and deduplicated URL frontier, and produces the raw HTML of each
page along with its outbound link graph — all implemented from scratch, with
no Scrapy or any third-party crawling library.

## What problem it solves

Any search engine needs a corpus first. Getting one right is not trivial: you
have to avoid crawling the same URL twice due to trivial variations
(uppercase, default port, query parameter order...), avoid overloading or
taking down third-party servers, recover from transient network failures
without losing progress, and survive broken or unreachable HTML without the
whole process crashing. This project solves exactly that layer: polite,
resilient ingestion of web pages.

## Role in `beacon-search-engine`

```text
                        ┌──────────────────────────┐
                        │  web-crawler-scheduler    │   (this project)
                        │  seed URLs → pages        │
                        └────────────┬─────────────┘
                                     │ pages.jsonl (raw HTML + metadata)
                                     │ link_graph.jsonl (outbound link graph)
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
      html-content-extractor              pagerank-link-analysis
      (cleans HTML → text)                (page authority via the graph)
                    │
                    ▼
      inverted-index-builder → index-compression-codec → bm25-ranking-engine
                                                                   │
                                                                   ▼
                                                    (converges in beacon-search-console)
```

It's the entry point of the whole ecosystem: without crawled pages there is
no corpus, and without a corpus there is nothing to index or rank.

## Goal and skills demonstrated

- Real asynchronous programming with `asyncio`/`aiohttp`: thousands of
  controlled concurrent connections, not a sequential loop dressed up as async.
- Managing a priority queue (BFS frontier by default, prioritizable).
- Ethical `robots.txt` compliance (including fractional `Crawl-delay`, which
  the standard library's `urllib.robotparser` does not support).
- Exponential backoff with jitter on transient network errors.
- URL deduplication via normalized hashing at scale.
- Checkpointing and resuming long-running jobs without losing state.

## How it works

1. Seed URLs are loaded into the **frontier** (`PriorityFrontier`), a
   priority queue with FIFO tie-breaking — by default ordered by discovery
   depth (BFS).
2. A main loop spawns concurrent tasks (up to `max_concurrent_requests`)
   that pull entries off the frontier.
3. For each URL: deduplication is checked via normalized hash (`urlnorm`),
   `robots.txt` is consulted (`RobotsCache`, cached per origin), and a slot
   is acquired from the per-domain rate limiter (`DomainRateLimiter`, which
   enforces both maximum concurrency and the site's `Crawl-delay`).
4. The page is downloaded (`AiohttpFetcher`), with retries and exponential
   backoff on 429/5xx/timeouts; a permanent 4xx or exhausted retries
   discards the URL explicitly and auditably.
5. On a successful download, outbound links are extracted
   (`link_extractor`, via the standard library's `html.parser`) and new ones
   are enqueued, respecting `max_depth`.
6. Everything is persisted as JSONL (`pages.jsonl`, `link_graph.jsonl`,
   `discarded.jsonl`) and, if configured, a periodic checkpoint is written so
   the crawl can be resumed after an interruption.

## Architecture

```text
src/web_crawler_scheduler/
├── models.py          # dataclasses: CrawlConfig, PageRecord, LinkGraphEntry,
│                       # FrontierEntry, DiscardedUrl, CheckpointState, CrawlStats
├── protocols.py        # interfaces: RobotsPolicy, Frontier, Deduplicator,
│                       # RateLimiter, PageFetcher
├── urlnorm.py           # URL normalization + HashSetDeduplicator
├── robots.py             # RobotsCache: fetching, parsing and per-origin caching of robots.txt
├── rate_limiter.py         # DomainRateLimiter: per-domain concurrency and minimum delay
├── frontier.py              # PriorityFrontier: BFS priority queue with FIFO tie-break
├── fetcher.py                 # AiohttpFetcher: fetch with retries and exponential backoff
├── link_extractor.py            # outbound <a href> extraction from raw HTML
├── pipeline.py                    # CrawlPipeline: orchestrates everything above
├── recrawl.py                      # change-rate recrawl scheduling + conditional GET
└── __main__.py                     # CLI (`crawl`, `stats`, `recrawl`)
```

Every module is tested in isolation via the interfaces defined in
`protocols.py` and in-memory test doubles (`tests/conftest.py`) — no test
hits the real network.

## Requirements and installation

- Python `>=3.11`

```bash
git clone https://github.com/juanmmm21/web-crawler-scheduler.git
cd web-crawler-scheduler
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Usage (CLI)

```bash
# Fresh crawl
web-crawler-scheduler crawl \
  --seed https://example.com/ \
  --output-dir ./output \
  --max-pages 500 \
  --max-depth 3 \
  --max-concurrent-requests 20 \
  --max-concurrent-per-domain 2 \
  --min-delay-seconds 1.0

# Resume an interrupted crawl (uses the checkpoint.json in --output-dir)
web-crawler-scheduler crawl --seed https://example.com/ --output-dir ./output --resume

# Quick stats on an already-crawled pages.jsonl
web-crawler-scheduler stats ./output/pages.jsonl
```

It can also be invoked as a module: `python -m web_crawler_scheduler crawl ...`.

Main `crawl` flags: `--seed` (repeatable), `--seeds-file`, `--output-dir`,
`--max-pages`, `--max-depth`, `--max-concurrent-requests`,
`--max-concurrent-per-domain`, `--min-delay-seconds`, `--timeout-seconds`,
`--max-retries`, `--backoff-base-seconds`, `--backoff-max-seconds`,
`--user-agent`, `--ignore-robots`, `--checkpoint-every`, `--resume`.

### Recrawling by change rate (from `0.2.0`)

```bash
# Register the pages of a previous crawl, then revisit only the ones that are due
web-crawler-scheduler recrawl --state ./recrawl_state.json \
  --seed-pages ./output/pages.jsonl --output-dir ./recrawl-001

# Later runs only need the state file
web-crawler-scheduler recrawl --state ./recrawl_state.json --output-dir ./recrawl-002 --max-pages 200
```

Every page gets its own revisit interval from its observed change rate. Change
is modelled as a Poisson process (Cho and Garcia-Molina's model of crawler
freshness): after `n` revisits that detected `X` changes, with mean interval
`Ī`, the bias-reduced estimate is `λ = -ln((n - X + 0.5) / (n + 0.5)) / Ī`,
and the next interval is the one that gives a 50% chance the page changed by
then, `I = -ln(1 - 0.5) / λ`, clamped to `[--min-interval-hours,
--max-interval-days]` and never more than twice the previous interval (so a
stable page backs off 1, 2, 4... days instead of jumping straight to a month).

Revisits are conditional: if the last response carried `ETag` or
`Last-Modified`, the request sends `If-None-Match` / `If-Modified-Since` and a
`304 Not Modified` confirms "unchanged" without transferring the body. Servers
that send no validators (or ignore them and answer 200) are compared by the
SHA-256 of the body. robots.txt, `Crawl-delay` and the per-domain rate limiter
are the same components the normal crawl uses; a URL that robots.txt now
disallows stops being revisited. Transient failures (timeouts, 5xx after
retries) are retried after the minimum interval and do not count as a visit.

## Data formats

`--output-dir` produces three JSONL files (one line = one record) and a
checkpoint JSON:

**`pages.jsonl`** — a successfully crawled page:

```json
{
  "url": "http://example.com/",
  "final_url": "http://example.com/",
  "status_code": 200,
  "headers": {"Content-Type": "text/html; charset=utf-8"},
  "html": "<html>...</html>",
  "fetched_at": "2026-07-08T12:00:00+00:00",
  "depth": 0,
  "content_type": "text/html"
}
```

**`link_graph.jsonl`** — outbound links per page (consumed by
[`pagerank-link-analysis`](https://github.com/juanmmm21/pagerank-link-analysis)):

```json
{"url": "http://example.com/", "outlinks": ["http://example.com/a", "http://example.com/b"]}
```

**`discarded.jsonl`** — discarded URLs, for auditing:

```json
{
  "url": "http://example.com/private",
  "reason": "blocked by robots.txt",
  "outcome": "robots_disallowed",
  "attempts": 0,
  "discarded_at": "2026-07-08T12:00:00+00:00"
}
```

**Recrawl outputs** (`recrawl --output-dir`): `pages.jsonl` holds only the
pages whose content changed, in exactly the `pages.jsonl` format above, so
extraction and incremental indexing treat them like any other crawl output.
`removed.jsonl` lists pages that answered 404 or 410, for the index to
tombstone:

```json
{"url": "http://example.com/old", "status_code": 410}
```

**`recrawl_state.json`** — per-page recrawl state, sorted by URL and replaced
atomically after every run (`format_version` is checked on load):

```json
{
  "format_version": 1,
  "pages": [
    {
      "url": "http://example.com/",
      "depth": 0,
      "etag": "\"5f2a\"",
      "last_modified": "Mon, 02 Nov 2026 10:00:00 GMT",
      "content_sha256": "9b74c9897bac770ffc029102a200c5de...",
      "last_fetched_at": "2026-11-03T12:00:00+00:00",
      "next_due_at": "2026-11-05T12:00:00+00:00",
      "checks": 3,
      "changes": 1,
      "total_interval_seconds": 259200.0
    }
  ]
}
```

**`checkpoint.json`** — internal state used to resume (visited hashes,
pending frontier and pages crawled); internal format, not meant for external
consumption.

## Programmatic usage

```python
import asyncio
from pathlib import Path

import aiohttp

from web_crawler_scheduler.models import CrawlConfig
from web_crawler_scheduler.pipeline import CrawlPipeline


async def main() -> None:
    config = CrawlConfig(seed_urls=("https://example.com/",), max_pages=50, max_depth=2)
    output = Path("./output")
    async with aiohttp.ClientSession() as session:
        async with CrawlPipeline(
            config,
            session,
            pages_output_path=output / "pages.jsonl",
            link_graph_output_path=output / "link_graph.jsonl",
            discarded_output_path=output / "discarded.jsonl",
            checkpoint_path=output / "checkpoint.json",
        ) as pipeline:
            stats = await pipeline.run()
    print(stats.pages_crawled, stats.urls_discarded)


asyncio.run(main())
```

## Development

```bash
pytest
ruff check .
mypy --strict src/
```

Tests never perform real network requests: `tests/conftest.py` defines
`FakeClientSession`/`FakeResponse`, test doubles that implement the same
surface used by the code (`.get()` as an async context manager) to simulate
HTTP responses, connection errors and timeouts deterministically.

## Troubleshooting

- **The crawl is not progressing / is very slow:** check
  `--min-delay-seconds` and `--max-concurrent-per-domain` — if all seeds are
  on the same domain, per-domain rate limiting is the expected bottleneck
  (it's the polite policy, not a bug).
- **`RuntimeError: release() llamado para un dominio sin acquire() previo`**
  (internal messages stay in Spanish, per project convention): indicates
  incorrect use of `DomainRateLimiter` outside of `CrawlPipeline` (calling
  `release()` without a prior `acquire()` for that domain).
- **A site never gets crawled:** check `discarded.jsonl` — if the `outcome`
  is `robots_disallowed`, the site's own `robots.txt` forbids it; with
  repeated `server_error`/`timeout`, the origin may be down (see the 5xx
  handling in `robots.py`, which assumes "everything forbidden" when
  `robots.txt` itself is unreachable, not just individual pages).
- **`mypy` fails under `tests/`:** expected — only `src/` is type-checked in
  `--strict` mode; the test doubles use deliberately looser typing.

## License

MIT — see [`LICENSE`](./LICENSE).
