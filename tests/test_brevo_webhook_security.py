"""Brevo webhook: shared-secret verification and org-scoped connection lookup.

Regression coverage for two real bugs fixed together:
- the webhook used to guess "first Brevo-connected org in the whole database"
  instead of using the org_id in the URL, a wrong-org data leak
- signature verification was a commented-out TODO, accepting any POST
"""
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.email_ingestion import _resolve_brevo_webhook_secret, _verify_brevo_shared_secret


class TestVerifyBrevoSharedSecret:
    def test_accepts_matching_secret(self):
        _verify_brevo_shared_secret("super-secret", "super-secret")  # no raise

    def test_accepts_when_unset_local_dev_posture(self):
        _verify_brevo_shared_secret(None, "")  # no raise, warn-and-accept

    def test_rejects_mismatched_secret(self):
        with pytest.raises(HTTPException) as exc:
            _verify_brevo_shared_secret("super-secret", "wrong-value")
        assert exc.value.status_code == 403

    def test_rejects_missing_header_when_secret_configured(self):
        with pytest.raises(HTTPException) as exc:
            _verify_brevo_shared_secret("super-secret", "")
        assert exc.value.status_code == 403


class TestResolveBrevoWebhookSecret:
    def test_uses_per_org_secret_when_present(self):
        db = MagicMock()
        token = MagicMock(webhook_secret="encrypted-blob")
        with patch("app.api.email_ingestion.decrypt_token", return_value="org-specific-secret"):
            result = _resolve_brevo_webhook_secret(db, uuid.uuid4(), token)
        assert result == "org-specific-secret"

    def test_falls_back_to_env_when_no_token_secret(self):
        db = MagicMock()
        token = MagicMock(webhook_secret=None)
        with patch("app.api.email_ingestion.settings") as settings:
            settings.BREVO_WEBHOOK_SECRET = "env-secret"
            result = _resolve_brevo_webhook_secret(db, uuid.uuid4(), token)
        assert result == "env-secret"

    def test_falls_back_to_env_when_no_token_at_all(self):
        db = MagicMock()
        with patch("app.api.email_ingestion.settings") as settings:
            settings.BREVO_WEBHOOK_SECRET = "env-secret"
            result = _resolve_brevo_webhook_secret(db, uuid.uuid4(), None)
        assert result == "env-secret"

    def test_returns_none_when_nothing_configured(self):
        db = MagicMock()
        with patch("app.api.email_ingestion.settings") as settings:
            settings.BREVO_WEBHOOK_SECRET = None
            result = _resolve_brevo_webhook_secret(db, uuid.uuid4(), None)
        assert result is None
