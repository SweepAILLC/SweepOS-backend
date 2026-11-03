"""Unit tests for onboarding-survey content-selection tally aggregation."""
from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.content_conversion_service import get_top_selected_content


def _media(media_id, **kwargs):
    defaults = dict(
        id=media_id,
        ig_media_id=f"ig_{str(media_id)[:8]}",
        permalink="https://instagram.com/p/abc",
        thumbnail_url="https://example.com/thumb.jpg",
        caption="caption",
        transcript=None,
        posted_at=None,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _db_with(rows, media_rows):
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = rows
    db.query.return_value.filter.return_value.all.return_value = media_rows
    return db


class TestGetTopSelectedContent:
    def test_no_responses_returns_empty(self):
        db = _db_with(rows=[], media_rows=[])
        org_id = uuid.uuid4()

        result = get_top_selected_content(db, org_id)

        assert result == []

    def test_orders_by_selection_count_and_joins_media_fields(self):
        media_a, media_b = uuid.uuid4(), uuid.uuid4()
        rows = [
            SimpleNamespace(instagram_media_id=media_a, selection_count=5),
            SimpleNamespace(instagram_media_id=media_b, selection_count=2),
        ]
        media_rows = [
            _media(media_a, caption="top performer"),
            _media(media_b, caption="runner up"),
        ]
        db = _db_with(rows, media_rows)
        org_id = uuid.uuid4()

        result = get_top_selected_content(db, org_id, limit=5)

        assert [r["instagram_media_id"] for r in result] == [str(media_a), str(media_b)]
        assert [r["selection_count"] for r in result] == [5, 2]
        assert result[0]["caption"] == "top performer"
        assert result[0]["ig_media_id"] == media_rows[0].ig_media_id

    def test_scopes_query_by_org_id(self):
        org_id = uuid.uuid4()
        db = _db_with(rows=[], media_rows=[])

        get_top_selected_content(db, org_id, limit=5)

        params = db.execute.call_args[0][1]
        assert params["org_id"] == str(org_id)
        assert params["limit"] == 5

    def test_skips_media_deleted_since_selection(self):
        present, deleted = uuid.uuid4(), uuid.uuid4()
        rows = [
            SimpleNamespace(instagram_media_id=present, selection_count=3),
            SimpleNamespace(instagram_media_id=deleted, selection_count=1),
        ]
        # Only the still-existing post comes back from the InstagramMedia join.
        media_rows = [_media(present)]
        db = _db_with(rows, media_rows)

        result = get_top_selected_content(db, uuid.uuid4())

        assert len(result) == 1
        assert result[0]["instagram_media_id"] == str(present)
