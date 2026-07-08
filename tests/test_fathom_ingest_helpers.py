"""Unit tests for Fathom ingest helpers (parsing, media extraction, edge cases)."""
from app.services.fathom_ingest import (
    _extract_media_urls,
    _norm_email,
    summary_to_markdown,
    transcript_to_text,
)


class TestNormEmail:
    def test_lowercases_and_strips(self):
        assert _norm_email("  User@Example.COM  ") == "user@example.com"

    def test_none_and_empty(self):
        assert _norm_email(None) is None
        assert _norm_email("") is None


class TestTranscriptToText:
    def test_empty_inputs(self):
        assert transcript_to_text(None) == ""
        assert transcript_to_text("") == ""
        assert transcript_to_text([]) == ""

    def test_string_passthrough(self):
        assert transcript_to_text("raw text") == "raw text"

    def test_list_with_speakers(self):
        data = [
            {"speaker": {"display_name": "Rep"}, "text": "Hello"},
            {"speaker": {"display_name": "Client"}, "text": "Hi"},
        ]
        assert transcript_to_text(data) == "Rep: Hello\nClient: Hi"

    def test_skips_malformed_items(self):
        data = [{"text": "solo"}, "bad", {"speaker": {}, "text": "ok"}]
        assert "solo" in transcript_to_text(data)
        assert "ok" in transcript_to_text(data)


class TestSummaryToMarkdown:
    def test_markdown_formatted_dict(self):
        assert summary_to_markdown({"markdown_formatted": "# Title"}) == "# Title"

    def test_nested_summary_dict(self):
        payload = {"summary": {"markdown_formatted": "Nested"}}
        assert summary_to_markdown(payload) == "Nested"

    def test_string_fallback(self):
        assert summary_to_markdown("plain") == "plain"


class TestExtractMediaUrls:
    def test_prefers_share_link_url(self):
        meeting = {"share_link_url": "https://fathom.video/share/abc", "video_url": "https://v.example/x"}
        media = _extract_media_urls(meeting)
        assert media["share_url"] == "https://fathom.video/share/abc"
        assert media["video_url"] == "https://v.example/x"

    def test_ignores_non_http_values(self):
        assert _extract_media_urls({"share_url": "not-a-url"}) == {"share_url": None, "video_url": None}
