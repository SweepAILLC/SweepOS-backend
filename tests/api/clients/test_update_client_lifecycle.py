"""PATCH lifecycle uses manual meta so pipeline automation does not revert board moves."""
from __future__ import annotations

import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.api.clients.crud import update_client
from app.models.client import Client, LifecycleState
from app.schemas.client import ClientUpdate
from app.services.client_automation import META_LIFECYCLE_MANUAL_AT, META_LIFECYCLE_MANUAL_STAGE


def _client(*, lifecycle=LifecycleState.QUALIFIED):
    return Client(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        email="lead@example.com",
        lifecycle_state=lifecycle,
        meta={},
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


def test_patch_lifecycle_persists_state_and_manual_meta():
    org_id = uuid.uuid4()
    client = _client(lifecycle=LifecycleState.QUALIFIED)
    user = SimpleNamespace(selected_org_id=org_id, org_id=org_id)

    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = client

    updated = update_client(
        str(client.id),
        ClientUpdate(lifecycle_state="nurturing"),
        db=mock_db,
        current_user=user,
    )

    assert updated.lifecycle_state == LifecycleState.NURTURING
    assert isinstance(updated.meta, dict)
    assert META_LIFECYCLE_MANUAL_AT in updated.meta
    assert updated.meta[META_LIFECYCLE_MANUAL_STAGE] == "nurturing"
    assert mock_db.commit.called


def test_patch_invalid_lifecycle_returns_422():
    org_id = uuid.uuid4()
    client = _client()
    user = SimpleNamespace(selected_org_id=org_id, org_id=org_id)

    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = client

    bad_update = ClientUpdate.model_construct(lifecycle_state="not_a_real_stage")
    with pytest.raises(HTTPException) as exc:
        update_client(
            str(client.id),
            bad_update,
            db=mock_db,
            current_user=user,
        )
    assert exc.value.status_code == 422
