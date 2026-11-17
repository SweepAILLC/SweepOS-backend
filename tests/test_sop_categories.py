"""Unit tests for owner-managed SOP categories and video thumbnail URL parsing."""
from unittest.mock import MagicMock, patch

import pytest

from app.services import sop_categories as sc
from app.services.resource_documents import normalize_sop_category
from app.services.video_thumbnails import loom_video_id, resolve_thumbnails

LOOM_ID = "a" * 32


class TestLabelAndColor:
    def test_label_trimmed_and_collapsed(self):
        assert sc._clean_label("  Client   Success ") == "Client Success"

    def test_label_required(self):
        with pytest.raises(ValueError, match="label_required"):
            sc._clean_label("   ")

    def test_label_truncated(self):
        assert len(sc._clean_label("x" * 100)) == sc.MAX_LABEL_LEN

    def test_invalid_color_rejected(self):
        with pytest.raises(ValueError, match="invalid_color"):
            sc._clean_color("neon")

    def test_slugify(self):
        assert sc._slugify("Client Success!") == "client-success"
        assert sc._slugify("!!!") == "category"


class TestNormalizeSopCategory:
    def test_non_sop_category_is_none(self):
        assert normalize_sop_category(MagicMock(), "sales", "AI Skill") is None

    def test_unknown_slug_strict_raises(self):
        with patch("app.services.resource_documents.valid_category_slugs", return_value={"sales"}):
            with pytest.raises(ValueError, match="invalid_sop_category"):
                normalize_sop_category(MagicMock(), "ghost", "SOP")

    def test_unknown_slug_lenient_returns_none(self):
        with patch("app.services.resource_documents.valid_category_slugs", return_value={"sales"}):
            assert normalize_sop_category(MagicMock(), "ghost", "SOP", strict=False) is None

    def test_custom_slug_accepted(self):
        with patch("app.services.resource_documents.valid_category_slugs", return_value={"hiring"}):
            assert normalize_sop_category(MagicMock(), "Hiring", "SOP") == "hiring"


class TestLoomThumbnails:
    def test_parses_share_and_embed_urls(self):
        assert loom_video_id(f"https://www.loom.com/share/{LOOM_ID}?sid=abc") == LOOM_ID
        assert loom_video_id(f"https://loom.com/embed/{LOOM_ID}") == LOOM_ID

    def test_rejects_other_hosts_and_bad_ids(self):
        assert loom_video_id(f"https://evil.com/share/{LOOM_ID}") is None
        assert loom_video_id("https://www.loom.com/share/not-an-id") is None
        assert loom_video_id("javascript:alert(1)") is None

    def test_resolve_only_fetches_loom(self):
        url = f"https://www.loom.com/share/{LOOM_ID}"
        with patch(
            "app.services.video_thumbnails._fetch_loom_thumbnail", return_value="https://cdn.loom.com/t.gif"
        ) as fetch:
            out = resolve_thumbnails([url, "https://youtu.be/xyz", "not a url"])
        assert out[url] == "https://cdn.loom.com/t.gif"
        assert out["https://youtu.be/xyz"] is None
        fetch.assert_called_once_with(LOOM_ID)
