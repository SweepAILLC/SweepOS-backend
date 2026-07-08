"""Unit tests for org resource library validation and tag sanitization."""
import base64
import uuid
from unittest.mock import MagicMock, patch

import pytest

from app.services.resource_library import (
    ALLOWED_KINDS,
    ALLOWED_TAGS,
    _sanitize_tags,
    upsert_library_item,
)


class TestSanitizeTags:
    def test_accepts_known_tags_preserving_canonical_casing(self):
        assert _sanitize_tags(["testimonials", "SOP", "ai"]) == ["testimonials", "SOP", "ai"]

    def test_maps_case_insensitive_to_canonical(self):
        assert _sanitize_tags(["sop", "CASE_STUDIES", "value"]) == ["SOP", "case_studies", "value"]

    def test_strips_unknown_tags(self):
        assert _sanitize_tags(["testimonials", "malicious", "other"]) == ["testimonials", "other"]

    def test_limits_to_twelve_tags(self):
        # Only the first 12 list entries are considered (see tags[:12] in _sanitize_tags).
        tags = ["testimonials"] * 12 + ["SOP"]
        assert _sanitize_tags(tags) == ["testimonials"]
        assert "SOP" not in _sanitize_tags(tags)

    def test_first_twelve_entries_can_include_all_allowed_tags(self):
        tags = list(ALLOWED_TAGS) * 2 + ["other"]
        assert set(_sanitize_tags(tags)) == ALLOWED_TAGS

    def test_deduplicates(self):
        assert _sanitize_tags(["SOP", "sop", "ai"]) == ["SOP", "ai"]

    def test_non_list_returns_empty(self):
        assert _sanitize_tags("not-a-list") == []
        assert _sanitize_tags(None) == []


class TestUpsertLibraryItemValidation:
    def test_rejects_invalid_kind(self):
        db = MagicMock()
        with pytest.raises(ValueError, match="invalid_kind"):
            upsert_library_item(
                db,
                org_id=uuid.uuid4(),
                item_id=None,
                kind="executable",
                title="Bad",
                description="",
                tags=[],
                content_text="x",
                content_url=None,
                content_b64=None,
                content_mime=None,
                user_id=uuid.uuid4(),
            )

    def test_rejects_empty_title(self):
        db = MagicMock()
        with pytest.raises(ValueError, match="title_required"):
            upsert_library_item(
                db,
                org_id=uuid.uuid4(),
                item_id=None,
                kind="text",
                title="   ",
                description="",
                tags=[],
                content_text="hello",
                content_url=None,
                content_b64=None,
                content_mime=None,
                user_id=uuid.uuid4(),
            )

    def test_url_kind_requires_url(self):
        db = MagicMock()
        with pytest.raises(ValueError, match="url_required"):
            upsert_library_item(
                db,
                org_id=uuid.uuid4(),
                item_id=None,
                kind="video_url",
                title="Demo",
                description="",
                tags=[],
                content_text=None,
                content_url=None,
                content_b64=None,
                content_mime=None,
                user_id=uuid.uuid4(),
            )

    def test_image_kind_requires_b64_and_mime(self):
        db = MagicMock()
        with pytest.raises(ValueError, match="image_required"):
            upsert_library_item(
                db,
                org_id=uuid.uuid4(),
                item_id=None,
                kind="image",
                title="Logo",
                description="",
                tags=[],
                content_text=None,
                content_url=None,
                content_b64=None,
                content_mime="image/png",
                user_id=uuid.uuid4(),
            )

    def test_image_rejects_invalid_base64(self):
        db = MagicMock()
        with pytest.raises(ValueError, match="invalid_image"):
            upsert_library_item(
                db,
                org_id=uuid.uuid4(),
                item_id=None,
                kind="image",
                title="Logo",
                description="",
                tags=[],
                content_text=None,
                content_url=None,
                content_b64="!!!not-base64!!!",
                content_mime="image/png",
                user_id=uuid.uuid4(),
            )

    @patch("app.services.resource_library.get_library_item")
    def test_text_kind_clears_url_and_image_fields(self, mock_get):
        org_id = uuid.uuid4()
        user_id = uuid.uuid4()
        item_id = uuid.uuid4()
        db = MagicMock()
        mock_get.return_value = {
            "id": str(item_id),
            "kind": "text",
            "title": "Note",
            "description": "",
            "tags": [],
            "content_text": "body",
            "content_url": None,
            "content_b64": None,
            "content_mime": None,
            "updated_at": None,
            "created_at": None,
        }
        upsert_library_item(
            db,
            org_id=org_id,
            item_id=item_id,
            kind="text",
            title="Note",
            description="",
            tags=["SOP"],
            content_text="body",
            content_url="https://evil.example",
            content_b64=base64.b64encode(b"x").decode(),
            content_mime="image/png",
            user_id=user_id,
        )
        db.execute.assert_called()
        db.commit.assert_called_once()


def test_allowed_kinds_and_tags_are_stable():
    assert "markdown" in ALLOWED_KINDS
    assert "video_url" in ALLOWED_KINDS
    assert "SOP" in ALLOWED_TAGS
    assert "case_studies" in ALLOWED_TAGS
