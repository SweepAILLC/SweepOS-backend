"""Calendly webhook registration: payload shape, delete ordering, and host safety.

Uses a fake httpx client, so nothing here touches the network.
"""
from unittest.mock import patch

from app.services import calendar_webhook_onboard as onboard
from app.services.calendar_webhook_onboard import _calendly_subscription_uri, _register_calendly

ORG = "https://api.calendly.com/organizations/ORG1"
USER = "https://api.calendly.com/users/USER1"
DEST = "https://api.example.com/webhooks/calendly/abc"
TOKEN = "tok-secret-123"


class FakeResponse:
    def __init__(self, status_code, body=None, text=""):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.text = text or str(body)

    def json(self):
        return self._body


class FakeClient:
    def __init__(self, post_responses, me=None):
        self.calls = []
        self._posts = list(post_responses)
        self._me = me or FakeResponse(200, {"resource": {"uri": USER, "current_organization": ORG}})

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, url, headers=None):
        self.calls.append(("GET", url, None))
        return self._me

    def post(self, url, headers=None, json=None):
        self.calls.append(("POST", url, json))
        return self._posts.pop(0)

    def delete(self, url, headers=None):
        self.calls.append(("DELETE", url, None))
        return FakeResponse(204)

    def methods(self):
        return [c[0] for c in self.calls]

    def posts(self):
        return [c[2] for c in self.calls if c[0] == "POST"]


def _run(client, existing_id=None):
    with patch.object(onboard.httpx, "Client", return_value=client):
        return _register_calendly(TOKEN, DEST, existing_id)


def _created(hook_id="NEW1"):
    return FakeResponse(201, {"resource": {"uri": f"https://api.calendly.com/webhook_subscriptions/{hook_id}"}})


def _only_calendly_hosts(client):
    return all(c[1].startswith("https://api.calendly.com/") for c in client.calls)


class TestPayload:
    def test_organization_scope_sends_organization_and_no_user(self):
        client = FakeClient([_created()])
        result = _run(client)
        assert result["success"] is True
        payload = client.posts()[0]
        assert payload["scope"] == "organization"
        assert payload["organization"] == ORG
        assert "user" not in payload
        assert payload["signing_key"] == result["secret"]

    def test_user_scope_fallback_still_sends_organization(self):
        client = FakeClient([FakeResponse(403, text="forbidden"), _created()])
        result = _run(client)
        assert result["success"] is True
        fallback = client.posts()[1]
        assert fallback["scope"] == "user"
        assert fallback["organization"] == ORG
        assert fallback["user"] == USER

    def test_missing_organization_uri_fails_without_posting(self):
        me = FakeResponse(200, {"resource": {"uri": USER}})
        client = FakeClient([], me=me)
        result = _run(client)
        assert result["success"] is False
        assert "organization" in result["error"]
        assert client.posts() == []

    def test_error_reports_both_attempts_and_leaks_no_secrets(self):
        client = FakeClient([FakeResponse(403, text="no org access"), FakeResponse(400, text="bad request")])
        result = _run(client)
        assert result["success"] is False
        assert "organization scope HTTP 403" in result["error"]
        assert "user scope HTTP 400" in result["error"]
        assert TOKEN not in result["error"]
        assert "signing_key" not in result["error"]
        assert "secret" not in result


class TestPreviousWebhookHandling:
    def test_previous_webhook_kept_when_creation_fails(self):
        client = FakeClient([FakeResponse(400, text="x"), FakeResponse(400, text="y")])
        result = _run(client, existing_id="OLD1")
        assert result["success"] is False
        assert "DELETE" not in client.methods()

    def test_previous_webhook_deleted_only_after_new_one_exists(self):
        client = FakeClient([_created("NEW1")])
        result = _run(client, existing_id="OLD1")
        assert result["success"] is True
        assert client.methods().index("POST") < client.methods().index("DELETE")
        delete_url = [c[1] for c in client.calls if c[0] == "DELETE"][0]
        assert delete_url.endswith("/webhook_subscriptions/OLD1")

    def test_same_id_is_not_deleted(self):
        client = FakeClient([_created("OLD1")])
        _run(client, existing_id="OLD1")
        assert "DELETE" not in client.methods()

    def test_duplicate_409_replaces_our_previous_webhook(self):
        client = FakeClient([FakeResponse(409, text="exists"), _created("NEW1")])
        result = _run(client, existing_id="OLD1")
        assert result["success"] is True
        assert client.methods() == ["GET", "POST", "DELETE", "POST"]

    def test_409_without_a_stored_id_never_deletes(self):
        client = FakeClient([FakeResponse(409, text="exists"), FakeResponse(409, text="exists")])
        result = _run(client)
        assert result["success"] is False
        assert "DELETE" not in client.methods()


class TestHostSafety:
    def test_foreign_host_id_is_never_requested(self):
        client = FakeClient([_created("NEW1")])
        result = _run(client, existing_id="https://evil.example/steal")
        assert result["success"] is True
        assert "DELETE" not in client.methods()
        assert _only_calendly_hosts(client)

    def test_all_requests_stay_on_calendly_api_host(self):
        client = FakeClient([_created("NEW1")])
        _run(client, existing_id="https://api.calendly.com/webhook_subscriptions/OLD1")
        assert _only_calendly_hosts(client)

    def test_uri_helper_accepts_bare_id_and_calendly_uri(self):
        expected = "https://api.calendly.com/webhook_subscriptions/OLD1"
        assert _calendly_subscription_uri("OLD1") == expected
        assert _calendly_subscription_uri("https://api.calendly.com/webhook_subscriptions/OLD1/") == expected

    def test_uri_helper_rejects_unsafe_values(self):
        assert _calendly_subscription_uri(None) is None
        assert _calendly_subscription_uri("") is None
        assert _calendly_subscription_uri("../users/me") is None
        assert _calendly_subscription_uri("abc/def") is None
        assert _calendly_subscription_uri("abc?x=1") is None
        assert _calendly_subscription_uri("a" * 65) is None
        assert _calendly_subscription_uri("https://evil.example/webhook_subscriptions/OLD1") is None
        assert _calendly_subscription_uri("https://api.calendly.com/webhook_subscriptions/../users/me") is None
