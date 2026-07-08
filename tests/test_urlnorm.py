from web_crawler_scheduler.urlnorm import (
    HashSetDeduplicator,
    extract_domain,
    normalize_url,
    url_hash,
)


class TestNormalizeUrl:
    def test_lowercases_scheme_and_host(self) -> None:
        assert normalize_url("HTTP://Example.COM/path") == "http://example.com/path"

    def test_strips_default_port(self) -> None:
        assert normalize_url("http://example.com:80/path") == "http://example.com/path"
        assert normalize_url("https://example.com:443/path") == "https://example.com/path"

    def test_keeps_non_default_port(self) -> None:
        assert normalize_url("http://example.com:8080/path") == "http://example.com:8080/path"

    def test_strips_fragment(self) -> None:
        assert normalize_url("http://example.com/path#section") == "http://example.com/path"

    def test_strips_trailing_slash_except_root(self) -> None:
        assert normalize_url("http://example.com/path/") == "http://example.com/path"
        assert normalize_url("http://example.com/") == "http://example.com/"
        assert normalize_url("http://example.com") == "http://example.com/"

    def test_sorts_query_parameters(self) -> None:
        left = normalize_url("http://example.com/path?b=2&a=1")
        right = normalize_url("http://example.com/path?a=1&b=2")
        assert left == right == "http://example.com/path?a=1&b=2"

    def test_equivalent_urls_normalize_identically(self) -> None:
        a = "HTTP://Example.com:80/foo/?z=1&a=2#ignored"
        b = "http://example.com/foo?a=2&z=1"
        assert normalize_url(a) == normalize_url(b)

    def test_distinct_paths_do_not_collide(self) -> None:
        assert normalize_url("http://example.com/foo") != normalize_url("http://example.com/bar")


class TestUrlHash:
    def test_stable_for_equivalent_urls(self) -> None:
        a = "http://example.com:80/foo/?b=2&a=1"
        b = "http://example.com/foo?a=1&b=2"
        assert url_hash(a) == url_hash(b)

    def test_different_for_distinct_urls(self) -> None:
        assert url_hash("http://example.com/foo") != url_hash("http://example.com/bar")

    def test_is_hex_sha256(self) -> None:
        digest = url_hash("http://example.com/")
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)


class TestExtractDomain:
    def test_extracts_host_lowercased(self) -> None:
        assert extract_domain("http://Example.COM/path") == "example.com"

    def test_strips_default_port(self) -> None:
        assert extract_domain("https://example.com:443/path") == "example.com"

    def test_keeps_non_default_port(self) -> None:
        assert extract_domain("http://example.com:8080/path") == "example.com:8080"


class TestHashSetDeduplicator:
    def test_new_url_not_seen(self) -> None:
        dedup = HashSetDeduplicator()
        assert dedup.seen("http://example.com/") is False

    def test_marks_seen_and_detects_duplicates(self) -> None:
        dedup = HashSetDeduplicator()
        dedup.mark_seen("http://example.com/path/")
        assert dedup.seen("http://example.com/path") is True

    def test_snapshot_restores_state(self) -> None:
        dedup = HashSetDeduplicator()
        dedup.mark_seen("http://example.com/path")
        restored = HashSetDeduplicator(initial_hashes=dedup.snapshot())
        assert restored.seen("http://example.com/path") is True

    def test_distinct_urls_are_independent(self) -> None:
        dedup = HashSetDeduplicator()
        dedup.mark_seen("http://example.com/foo")
        assert dedup.seen("http://example.com/bar") is False
