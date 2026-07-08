import json
from pathlib import Path

import pytest

from web_crawler_scheduler.models import CrawlConfig
from web_crawler_scheduler.pipeline import CrawlPipeline

from .conftest import FakeClientSession, FakeResponse

ROBOTS_ALLOW_ALL = FakeResponse(status=200, body="User-agent: *\nAllow: /\n")


def _config(seed_urls: tuple[str, ...], **overrides: object) -> CrawlConfig:
    fields: dict[str, object] = {
        "default_min_delay_seconds": 0.0,
        "backoff_base_seconds": 0.001,
        "backoff_max_seconds": 0.01,
        "request_timeout_seconds": 5.0,
        "max_retries": 1,
    }
    fields.update(overrides)
    return CrawlConfig(seed_urls=seed_urls, **fields)  # type: ignore[arg-type]


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text("utf-8").splitlines() if line]


class TestCrawlPipelineBasicCrawl:
    async def test_crawls_seed_and_discovered_links_within_depth(self, tmp_path: Path) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": ROBOTS_ALLOW_ALL,
                "http://example.com/": FakeResponse(
                    status=200,
                    body=(
                        '<a href="http://example.com/a">a</a>'
                        '<a href="http://example.com/b">b</a>'
                    ),
                    final_url="http://example.com/",
                ),
                "http://example.com/a": FakeResponse(
                    status=200, body="page a", final_url="http://example.com/a"
                ),
                "http://example.com/b": FakeResponse(
                    status=200, body="page b", final_url="http://example.com/b"
                ),
            }
        )
        config = _config(("http://example.com/",), max_depth=1)
        pages_path = tmp_path / "pages.jsonl"
        graph_path = tmp_path / "link_graph.jsonl"
        discarded_path = tmp_path / "discarded.jsonl"

        async with CrawlPipeline(
            config, session, pages_path, graph_path, discarded_path  # type: ignore[arg-type]
        ) as pipeline:
            stats = await pipeline.run()

        assert stats.pages_crawled == 3
        assert stats.urls_discarded == 0

        pages = _read_jsonl(pages_path)
        assert {p["url"] for p in pages} == {
            "http://example.com/",
            "http://example.com/a",
            "http://example.com/b",
        }

        graph = _read_jsonl(graph_path)
        seed_entry = next(g for g in graph if g["url"] == "http://example.com/")
        assert set(seed_entry["outlinks"]) == {  # type: ignore[arg-type]
            "http://example.com/a",
            "http://example.com/b",
        }

    async def test_does_not_exceed_max_depth(self, tmp_path: Path) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": ROBOTS_ALLOW_ALL,
                "http://example.com/": FakeResponse(
                    status=200,
                    body='<a href="http://example.com/a">a</a>',
                    final_url="http://example.com/",
                ),
                "http://example.com/a": FakeResponse(
                    status=200,
                    body='<a href="http://example.com/too-deep">nope</a>',
                    final_url="http://example.com/a",
                ),
            }
        )
        config = _config(("http://example.com/",), max_depth=1)
        pages_path = tmp_path / "pages.jsonl"

        async with CrawlPipeline(
            config,
            session,  # type: ignore[arg-type]
            pages_path,
            tmp_path / "graph.jsonl",
            tmp_path / "discarded.jsonl",
        ) as pipeline:
            stats = await pipeline.run()

        assert stats.pages_crawled == 2
        urls = {p["url"] for p in _read_jsonl(pages_path)}
        assert "http://example.com/too-deep" not in urls

    async def test_respects_max_pages(self, tmp_path: Path) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": ROBOTS_ALLOW_ALL,
                "http://example.com/": FakeResponse(
                    status=200,
                    body=(
                        '<a href="http://example.com/a">a</a>'
                        '<a href="http://example.com/b">b</a>'
                    ),
                    final_url="http://example.com/",
                ),
                "http://example.com/a": FakeResponse(status=200, body="a"),
                "http://example.com/b": FakeResponse(status=200, body="b"),
            }
        )
        config = _config(("http://example.com/",), max_depth=1, max_pages=1)

        async with CrawlPipeline(
            config,
            session,  # type: ignore[arg-type]
            tmp_path / "pages.jsonl",
            tmp_path / "graph.jsonl",
            tmp_path / "discarded.jsonl",
        ) as pipeline:
            stats = await pipeline.run()

        assert stats.pages_crawled == 1


class TestCrawlPipelineRobots:
    async def test_discards_url_disallowed_by_robots(self, tmp_path: Path) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nDisallow: /private\n"
                ),
                "http://example.com/private": FakeResponse(status=200, body="secret"),
            }
        )
        config = _config(("http://example.com/private",))
        discarded_path = tmp_path / "discarded.jsonl"

        async with CrawlPipeline(
            config,
            session,  # type: ignore[arg-type]
            tmp_path / "pages.jsonl",
            tmp_path / "graph.jsonl",
            discarded_path,
        ) as pipeline:
            stats = await pipeline.run()

        assert stats.pages_crawled == 0
        assert stats.urls_discarded == 1
        discarded = _read_jsonl(discarded_path)
        assert discarded[0]["outcome"] == "robots_disallowed"
        assert discarded[0]["url"] == "http://example.com/private"

    async def test_ignores_robots_when_disabled_in_config(self, tmp_path: Path) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": FakeResponse(
                    status=200, body="User-agent: *\nDisallow: /private\n"
                ),
                "http://example.com/private": FakeResponse(status=200, body="secret"),
            }
        )
        config = _config(("http://example.com/private",), obey_robots_txt=False)

        async with CrawlPipeline(
            config,
            session,  # type: ignore[arg-type]
            tmp_path / "pages.jsonl",
            tmp_path / "graph.jsonl",
            tmp_path / "discarded.jsonl",
        ) as pipeline:
            stats = await pipeline.run()

        assert stats.pages_crawled == 1
        assert stats.urls_discarded == 0


class TestCrawlPipelineFetchFailures:
    async def test_discards_url_after_exhausting_retries(self, tmp_path: Path) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": ROBOTS_ALLOW_ALL,
                "http://example.com/broken": FakeResponse(status=503),
            }
        )
        config = _config(("http://example.com/broken",), max_retries=1)
        discarded_path = tmp_path / "discarded.jsonl"

        async with CrawlPipeline(
            config,
            session,  # type: ignore[arg-type]
            tmp_path / "pages.jsonl",
            tmp_path / "graph.jsonl",
            discarded_path,
        ) as pipeline:
            stats = await pipeline.run()

        assert stats.pages_crawled == 0
        assert stats.urls_discarded == 1
        discarded = _read_jsonl(discarded_path)
        assert discarded[0]["outcome"] == "discarded_after_retries"


class TestCrawlPipelineCheckpoint:
    async def test_checkpoint_and_resume_completes_remaining_frontier(
        self, tmp_path: Path
    ) -> None:
        session = FakeClientSession(
            {
                "http://example.com/robots.txt": ROBOTS_ALLOW_ALL,
                "http://example.com/": FakeResponse(
                    status=200,
                    body=(
                        '<a href="http://example.com/a">a</a>'
                        '<a href="http://example.com/b">b</a>'
                    ),
                    final_url="http://example.com/",
                ),
                "http://example.com/a": FakeResponse(
                    status=200, body="a", final_url="http://example.com/a"
                ),
                "http://example.com/b": FakeResponse(
                    status=200, body="b", final_url="http://example.com/b"
                ),
            }
        )
        pages_path = tmp_path / "pages.jsonl"
        graph_path = tmp_path / "graph.jsonl"
        discarded_path = tmp_path / "discarded.jsonl"
        checkpoint_path = tmp_path / "checkpoint.json"

        first_config = _config(("http://example.com/",), max_depth=1, max_pages=1)
        async with CrawlPipeline(
            first_config,
            session,  # type: ignore[arg-type]
            pages_path,
            graph_path,
            discarded_path,
            checkpoint_path=checkpoint_path,
        ) as pipeline:
            first_stats = await pipeline.run()

        assert first_stats.pages_crawled == 1
        assert checkpoint_path.exists()

        resume_config = _config(("http://example.com/",), max_depth=1, max_pages=100)
        async with CrawlPipeline.resume_from_checkpoint(
            resume_config,
            session,  # type: ignore[arg-type]
            pages_path,
            graph_path,
            discarded_path,
            checkpoint_path=checkpoint_path,
        ) as resumed_pipeline:
            second_stats = await resumed_pipeline.run()

        assert second_stats.pages_crawled == 3  # acumulado: seed (sesión previa) + a + b

        urls = sorted(p["url"] for p in _read_jsonl(pages_path))  # type: ignore[type-var]
        assert urls == [
            "http://example.com/",
            "http://example.com/a",
            "http://example.com/b",
        ]


class TestCrawlPipelineValidation:
    def test_rejects_non_positive_checkpoint_every(self, tmp_path: Path) -> None:
        config = _config(("http://example.com/",))
        session = FakeClientSession({})
        with pytest.raises(ValueError):
            CrawlPipeline(
                config,
                session,  # type: ignore[arg-type]
                tmp_path / "pages.jsonl",
                tmp_path / "graph.jsonl",
                tmp_path / "discarded.jsonl",
                checkpoint_every=0,
            )
