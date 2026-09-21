"""GHL identity-merge logic (pure functions; DB-query paths reuse already-tested
find_client_by_email/find_client_by_phone from app.models.client)."""
from types import SimpleNamespace

from app.services.ghl_sync_service import apply_ghl_identity_to_client, _stamp_ghl_contact_id


class TestApplyGhlIdentityToClient:
    def test_fills_missing_email_and_phone(self):
        client = SimpleNamespace(email=None, emails=None, phone=None, first_name=None, last_name=None, updated_at=None)
        apply_ghl_identity_to_client(client, "lead@example.com", "+15551234567", "Jane", "Doe")
        assert client.email == "lead@example.com"
        assert client.phone == "+15551234567"
        assert client.first_name == "Jane"
        assert client.last_name == "Doe"

    def test_never_overwrites_existing_email_or_name(self):
        client = SimpleNamespace(
            email="original@example.com",
            emails=[],
            phone="+10005550000",
            first_name="Original",
            last_name="Name",
            updated_at=None,
        )
        apply_ghl_identity_to_client(client, "new@example.com", "+19998887777", "New", "Person")
        assert client.email == "original@example.com"
        assert client.phone == "+10005550000"
        assert client.first_name == "Original"
        assert client.last_name == "Name"
        # differing email is appended as a secondary email, never dropped
        assert client.emails == ["new@example.com"]

    def test_duplicate_secondary_email_not_appended_twice(self):
        client = SimpleNamespace(
            email="original@example.com",
            emails=["new@example.com"],
            phone=None,
            first_name=None,
            last_name=None,
            updated_at=None,
        )
        apply_ghl_identity_to_client(client, "new@example.com", None, None, None)
        assert client.emails == ["new@example.com"]

    def test_blank_inputs_change_nothing(self):
        client = SimpleNamespace(email=None, emails=None, phone=None, first_name=None, last_name=None, updated_at=None)
        apply_ghl_identity_to_client(client, None, None, None, None)
        assert client.email is None
        assert client.phone is None


class TestStampGhlContactId:
    def test_sets_meta_when_absent(self):
        client = SimpleNamespace(meta=None)
        _stamp_ghl_contact_id(client, "ghl_123")
        assert client.meta == {"ghl_contact_id": "ghl_123"}

    def test_preserves_other_meta_keys(self):
        client = SimpleNamespace(meta={"other": "value"})
        _stamp_ghl_contact_id(client, "ghl_123")
        assert client.meta == {"other": "value", "ghl_contact_id": "ghl_123"}

    def test_noop_when_id_missing(self):
        client = SimpleNamespace(meta={"other": "value"})
        _stamp_ghl_contact_id(client, None)
        assert client.meta == {"other": "value"}

    def test_noop_when_already_stamped(self):
        client = SimpleNamespace(meta={"ghl_contact_id": "ghl_123"})
        original = client.meta
        _stamp_ghl_contact_id(client, "ghl_123")
        assert client.meta is original
