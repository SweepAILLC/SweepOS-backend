"""Custom funnel webhooks: parsing, field detection, dedupe keys, and the public
endpoint's status codes (mock db; the inbox insert and processing are patched)."""
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import funnel_webhooks as api
from app.db.session import get_db
from app.services import funnel_webhooks as fw


class TestParseBody:
    def test_json_object(self):
        assert fw.parse_body("application/json", b'{"email": "a@b.co"}') == {"email": "a@b.co"}

    def test_single_item_array_unwrapped(self):
        assert fw.parse_body("application/json", b'[{"email": "a@b.co"}]') == {"email": "a@b.co"}

    def test_form_urlencoded_with_repeated_keys(self):
        out = fw.parse_body("application/x-www-form-urlencoded", b"email=a%40b.co&goal=x&goal=y")
        assert out == {"email": "a@b.co", "goal": ["x", "y"]}

    def test_missing_content_type_sniffs_json(self):
        assert fw.parse_body(None, b'{"a": 1}') == {"a": 1}

    @pytest.mark.parametrize(
        "ctype,body",
        [
            ("multipart/form-data; boundary=x", b"--x"),
            ("application/json", b"not json"),
            ("application/json", b"[1, 2]"),
            ("application/json", b"   "),
        ],
    )
    def test_rejects(self, ctype, body):
        with pytest.raises(fw.WebhookPayloadError):
            fw.parse_body(ctype, body)


class TestNormalize:
    def test_aliases_on_flat_payload(self):
        lead = fw.normalize_payload(
            {"Email Address": "ADA@Example.com", "Phone Number": "+1 (555) 010-2000", "First Name": "Ada",
             "utm_source": "fb", "Biggest struggle": "leads"}
        )
        assert lead.fields == {"email": "ada@example.com", "phone": "+1 (555) 010-2000", "first_name": "Ada"}
        assert lead.utm == {"source": "fb"}
        assert lead.opt_in_data == {"Biggest struggle": "leads"}
        assert lead.has_identity

    def test_nested_shallowest_match_wins(self):
        lead = fw.normalize_payload({"owner": {"email": "coach@x.co"}, "email": "lead@x.co"})
        assert lead.fields["email"] == "lead@x.co"

    def test_label_value_list(self):
        lead = fw.normalize_payload(
            {"fields": [{"name": "email", "value": "a@b.co"}, {"label": "Revenue", "value": "10k"}]}
        )
        assert lead.fields["email"] == "a@b.co"
        assert lead.opt_in_data == {"fields.Revenue": "10k"}

    def test_field_map_beats_aliases_and_supports_indices(self):
        payload = {"email": "wrong@x.co", "answers": [{"text": "right@x.co"}]}
        lead = fw.normalize_payload(payload, {"email": "answers.0.text"})
        assert lead.fields["email"] == "right@x.co"

    def test_invalid_email_and_short_phone_ignored(self):
        lead = fw.normalize_payload({"email": "nope", "phone": "123"})
        assert lead.fields == {}
        assert not lead.has_identity

    def test_opt_in_budget_is_bounded(self):
        lead = fw.normalize_payload({"email": "a@b.co", **{f"q{i}": "x" * 3000 for i in range(100)}})
        assert sum(len(k) + len(str(v)) for k, v in lead.opt_in_data.items()) < fw._OPT_IN_BUDGET


class TestDedupeKey:
    def test_header_first(self):
        assert fw.dedupe_key({"idempotency-key": "abc"}, {"event_id": "z"}) == "h:abc"

    def test_body_id(self):
        assert fw.dedupe_key({}, {"submission_id": 42}) == "b:42"

    def test_body_hash_ignores_key_order(self):
        assert fw.dedupe_key({}, {"a": 1, "b": 2}) == fw.dedupe_key({}, {"b": 2, "a": 1})
        assert fw.dedupe_key({}, {"a": 1}) != fw.dedupe_key({}, {"a": 2})


class TestTokens:
    def test_generated_token_matches_format(self):
        tok = fw.generate_token()
        assert fw._TOKEN_RE.match(tok)
        assert len(fw.hash_token(tok)) == 64

    def test_malformed_token_skips_db(self):
        db = MagicMock()
        assert fw.resolve_target(db, "not-a-token") is None
        db.query.assert_not_called()

    def test_field_map_rejects_unknown_target(self):
        with pytest.raises(fw.WebhookPayloadError):
            fw.set_field_map(MagicMock(), MagicMock(webhook_config={}), {"password": "x"})


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(api.router, prefix="/webhooks")
    app.dependency_overrides[get_db] = lambda: MagicMock()
    return TestClient(app)


TARGET = fw.WebhookTarget(funnel_id=uuid.uuid4(), org_id=uuid.uuid4())
TOKEN = "swh_" + "a" * 43


class TestEndpoint:
    def test_unknown_token_404(self, client):
        with patch.object(fw, "resolve_target", return_value=None), patch.object(fw, "try_acquire_bad_token", return_value=True):
            assert client.post(f"/webhooks/funnels/{TOKEN}", json={"email": "a@b.co"}).status_code == 404

    def test_unknown_token_flood_429(self, client):
        with patch.object(fw, "resolve_target", return_value=None), patch.object(fw, "try_acquire_bad_token", return_value=False):
            assert client.post(f"/webhooks/funnels/{TOKEN}", json={}).status_code == 429

    def test_rate_limited_429_with_retry_after(self, client):
        with patch.object(fw, "resolve_target", return_value=TARGET), patch.object(fw, "try_acquire_rate", return_value=False):
            r = client.post(f"/webhooks/funnels/{TOKEN}", json={"email": "a@b.co"})
        assert r.status_code == 429
        assert r.headers["retry-after"] == "60"

    def test_no_identity_422(self, client):
        with patch.object(fw, "resolve_target", return_value=TARGET), patch.object(fw, "try_acquire_rate", return_value=True):
            assert client.post(f"/webhooks/funnels/{TOKEN}", json={"foo": "bar"}).status_code == 422

    def test_too_large_413(self, client):
        with patch.object(fw, "resolve_target", return_value=TARGET), patch.object(
            fw, "try_acquire_rate", return_value=True
        ), patch.object(api.settings, "FUNNEL_WEBHOOK_MAX_BODY_BYTES", 100):
            r = client.post(f"/webhooks/funnels/{TOKEN}", json={"email": "a@b.co", "x": "y" * 500})
        assert r.status_code == 413

    @pytest.mark.parametrize("inline", [False, True])
    def test_accepted_enqueues_and_processes_per_mode(self, client, inline):
        row_id = uuid.uuid4()
        with patch.object(fw, "resolve_target", return_value=TARGET), patch.object(
            fw, "try_acquire_rate", return_value=True
        ), patch.object(fw, "enqueue_delivery", return_value=(row_id, False)) as enq, patch.object(
            fw, "process_delivery_now"
        ) as proc, patch.object(fw, "inline_enabled", return_value=inline):
            r = client.post(f"/webhooks/funnels/{TOKEN}", data={"email": "a@b.co", "name": "Ada L"})
        assert r.status_code == 202
        assert r.json()["id"] == str(row_id)
        assert r.json()["detected"]["email"] is True
        enq.assert_called_once()
        # Default (worker mode) leaves processing to the drainer; inline runs it after the 202.
        assert proc.call_count == (1 if inline else 0)

    def test_duplicate_acked_without_processing(self, client):
        with patch.object(fw, "resolve_target", return_value=TARGET), patch.object(
            fw, "try_acquire_rate", return_value=True
        ), patch.object(fw, "enqueue_delivery", return_value=(None, True)), patch.object(fw, "process_delivery_now") as proc:
            r = client.post(f"/webhooks/funnels/{TOKEN}", json={"email": "a@b.co"})
        assert r.status_code == 202
        assert r.json()["duplicate"] is True
        proc.assert_not_called()
