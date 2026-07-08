import json
from pathlib import Path

import pytest

from web_crawler_scheduler.__main__ import _build_parser, _load_seed_urls, _run_crawl, _run_stats


class TestLoadSeedUrls:
    def test_combines_inline_seeds_and_file(self, tmp_path: Path) -> None:
        seeds_file = tmp_path / "seeds.txt"
        seeds_file.write_text("http://example.com/a\n\nhttp://example.com/b\n", encoding="utf-8")

        urls = _load_seed_urls(["http://example.com/inline"], seeds_file)

        assert urls == (
            "http://example.com/inline",
            "http://example.com/a",
            "http://example.com/b",
        )

    def test_no_file_returns_only_inline_seeds(self) -> None:
        assert _load_seed_urls(["http://example.com/"], None) == ("http://example.com/",)

    def test_blank_lines_in_file_are_skipped(self, tmp_path: Path) -> None:
        seeds_file = tmp_path / "seeds.txt"
        seeds_file.write_text("\n  \nhttp://example.com/only\n\n", encoding="utf-8")

        assert _load_seed_urls([], seeds_file) == ("http://example.com/only",)


class TestArgumentParsing:
    def test_crawl_requires_output_dir(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["crawl", "--seed", "http://example.com/"])

    def test_crawl_parses_defaults(self, tmp_path: Path) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            ["crawl", "--seed", "http://example.com/", "--output-dir", str(tmp_path)]
        )
        assert args.command == "crawl"
        assert args.seeds == ["http://example.com/"]
        assert args.max_pages == 1000
        assert args.max_depth == 3
        assert args.ignore_robots is False
        assert args.resume is False

    def test_stats_requires_pages_path(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["stats"])

    def test_unknown_command_exits(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["bogus"])


class TestRunCrawlValidation:
    def test_fails_without_any_seed_url(self, tmp_path: Path) -> None:
        parser = _build_parser()
        args = parser.parse_args(["crawl", "--output-dir", str(tmp_path)])

        assert _run_crawl(args) == 2


class TestRunStats:
    def test_reports_missing_file(self, tmp_path: Path) -> None:
        parser = _build_parser()
        args = parser.parse_args(["stats", str(tmp_path / "missing.jsonl")])

        assert _run_stats(args) == 2

    def test_summarizes_pages_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        pages_path = tmp_path / "pages.jsonl"
        records = [
            {"status_code": 200, "html": "a" * 10},
            {"status_code": 200, "html": "b" * 20},
            {"status_code": 404, "html": ""},
        ]
        pages_path.write_text(
            "\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8"
        )
        parser = _build_parser()
        args = parser.parse_args(["stats", str(pages_path)])

        exit_code = _run_stats(args)

        assert exit_code == 0
        output = capsys.readouterr().out
        assert "Páginas: 3" in output
        assert "HTTP 200: 2" in output
        assert "HTTP 404: 1" in output
