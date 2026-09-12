"""
Regression coverage for the invite-accept account-takeover fix.

Root issue: an OPEN/unbound invite link (invitee_email unset — e.g. the
platform-admin-minted org signup link) lets the ACCEPTOR choose any email.
Before this fix, if that email matched an existing user, accept_invitation
issued a fully valid access_token for that account with zero password check —
a password-free account takeover, since org access elsewhere
(auth.py::switch_organization) is resolved by email.

These tests exercise the extracted guard directly (SimpleNamespace fakes, no
DB/FastAPI — matches this suite's existing style, see
test_organization_invitations.py) rather than the full accept_invitation
endpoint, which would need a much larger DB double for parts unrelated to
this security property.
"""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api.auth import _require_password_proof_for_open_invite
from app.core.security import get_password_hash


def _existing_user(password: str | None):
    hashed = get_password_hash(password) if password else None
    return SimpleNamespace(hashed_password=hashed)


def test_bound_invite_never_requires_password():
    """Admin explicitly typed this email — trusted input, no password needed."""
    user = _existing_user(None)  # even a Google-only account is fine here
    _require_password_proof_for_open_invite(False, user, None)  # must not raise


def test_open_invite_blocks_missing_password():
    user = _existing_user("correct horse battery staple")
    with pytest.raises(HTTPException) as exc:
        _require_password_proof_for_open_invite(True, user, None)
    assert exc.value.status_code == 401


def test_open_invite_blocks_wrong_password():
    user = _existing_user("correct horse battery staple")
    with pytest.raises(HTTPException) as exc:
        _require_password_proof_for_open_invite(True, user, "guessed-wrong")
    assert exc.value.status_code == 401


def test_open_invite_accepts_correct_password():
    user = _existing_user("correct horse battery staple")
    _require_password_proof_for_open_invite(
        True, user, "correct horse battery staple"
    )  # must not raise


def test_open_invite_blocks_google_only_account_with_clear_message():
    """No hashed_password at all (Google sign-in) — there is nothing to verify
    against, so this must block rather than silently accept."""
    user = _existing_user(None)
    with pytest.raises(HTTPException) as exc:
        _require_password_proof_for_open_invite(True, user, "anything")
    assert exc.value.status_code == 400
    assert "Google" in exc.value.detail
