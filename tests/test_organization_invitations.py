from types import SimpleNamespace

import pytest

from app.services.organization_invitations import invitation_accept_link, resolve_invitation_email


def test_invitation_accept_link_uses_frontend_url(monkeypatch):
    from app.core import config

    monkeypatch.setattr(config.settings, "FRONTEND_URL", "https://app.example.com/")
    assert invitation_accept_link("tok_abc") == "https://app.example.com/invite/accept?token=tok_abc"


def test_resolve_open_invite_requires_provided_email():
    inv = SimpleNamespace(invitee_email=None)
    with pytest.raises(Exception):
        resolve_invitation_email(inv, None)
    assert resolve_invitation_email(inv, "new@example.com") == "new@example.com"


def test_invitation_never_expires():
    from app.services.organization_invitations import (
        invitation_is_expired,
        resolve_invitation_expires_at,
    )

    assert resolve_invitation_expires_at(never_expires=True) is None
    assert invitation_is_expired(SimpleNamespace(expires_at=None)) is False


def test_consume_invitation_skips_multi_use():
    from app.services.organization_invitations import consume_invitation

    inv = SimpleNamespace(multi_use=True, used_at=None)
    consume_invitation(inv)
    assert inv.used_at is None
    single = SimpleNamespace(multi_use=False, used_at=None)
    consume_invitation(single)
    assert single.used_at is not None


def test_ensure_invite_organization_requires_name_when_open():
    from fastapi import HTTPException

    from app.services.organization_invitations import ensure_invite_organization

    inv = SimpleNamespace(org_id=None)
    with pytest.raises(HTTPException):
        ensure_invite_organization(None, inv, None)


def test_resolve_bound_invite_rejects_mismatch():
    inv = SimpleNamespace(invitee_email="bound@example.com")
    assert resolve_invitation_email(inv, None) == "bound@example.com"
    with pytest.raises(Exception):
        resolve_invitation_email(inv, "other@example.com")
