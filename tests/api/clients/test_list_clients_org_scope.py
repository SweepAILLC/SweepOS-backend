"""Ensure list_clients scopes queries to the JWT-selected org."""
from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.api.clients.crud import list_clients
from app.api.clients.helpers import scope_org_id
from app.models.client import Client


def test_scope_org_id_prefers_selected_org():
    selected = uuid.uuid4()
    primary = uuid.uuid4()
    user = SimpleNamespace(selected_org_id=selected, org_id=primary)
    assert scope_org_id(user) == selected


def test_scope_org_id_falls_back_to_primary_org():
    primary = uuid.uuid4()
    user = SimpleNamespace(selected_org_id=None, org_id=primary)
    assert scope_org_id(user) == primary


def test_list_clients_filters_by_scoped_org_id():
    selected_org = uuid.uuid4()
    primary_org = uuid.uuid4()
    user = SimpleNamespace(selected_org_id=selected_org, org_id=primary_org)

    mock_query = MagicMock()
    mock_db = MagicMock()
    mock_db.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = []

    list_clients(db=mock_db, current_user=user, lifecycle_state=None)

    assert mock_query.filter.called
    org_filters = [
        call[0][0]
        for call in mock_query.filter.call_args_list
        if getattr(getattr(call[0][0], "left", None), "key", None) == "org_id"
    ]
    assert len(org_filters) == 1
    assert org_filters[0].left.table.name == Client.__tablename__
    assert org_filters[0].right.value == selected_org
