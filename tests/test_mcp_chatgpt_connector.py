"""ChatGPT connector support: confidential DCR clients, Basic client auth, tool annotations, fetch ids."""
import base64
import uuid
from types import SimpleNamespace

from starlette.requests import Request

from app.api.mcp_oauth import _basic_client_credentials
from app.core.encryption import encrypt_token
from app.mcp.server import SUPPORTED_PROTOCOL_VERSIONS, TOOLS
from app.services import mcp_oauth_service as svc
from app.services.mcp_search_fetch import fetch_for_mcp, search_for_mcp


def _request(headers: dict) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "headers": raw})


def test_basic_client_credentials_parses_header():
    token = base64.b64encode(b"mcp_abc:s3cr%3At").decode()
    assert _basic_client_credentials(_request({"Authorization": f"Basic {token}"})) == ("mcp_abc", "s3cr:t")


def test_basic_client_credentials_ignores_bearer_and_garbage():
    assert _basic_client_credentials(_request({"Authorization": "Bearer x"})) == (None, None)
    assert _basic_client_credentials(_request({"Authorization": "Basic !!!"})) == (None, None)
    assert _basic_client_credentials(_request({})) == (None, None)


def test_verify_client_secret():
    public = SimpleNamespace(client_secret_encrypted=None)
    assert svc.verify_client_secret(public, None)  # PKCE-only public client

    confidential = SimpleNamespace(client_secret_encrypted=encrypt_token("right"))
    assert svc.verify_client_secret(confidential, "right")
    assert not svc.verify_client_secret(confidential, "wrong")
    assert not svc.verify_client_secret(confidential, None)


def test_auth_methods_advertised():
    assert set(svc.SUPPORTED_AUTH_METHODS) == {"none", "client_secret_post", "client_secret_basic"}


def test_chatgpt_protocol_version_supported():
    assert "2025-06-18" in SUPPORTED_PROTOCOL_VERSIONS


def test_search_and_fetch_tools_match_openai_shape():
    by_name = {t["name"]: t for t in TOOLS}
    assert by_name["search"]["inputSchema"]["required"] == ["query"]
    assert by_name["fetch"]["inputSchema"]["required"] == ["id"]


def test_only_email_send_is_not_read_only():
    writes = {t["name"] for t in TOOLS if not t["annotations"]["readOnlyHint"]}
    assert writes == {"send_client_email"}


def test_fetch_rejects_malformed_ids_without_db():
    org = uuid.uuid4()
    assert "error" in fetch_for_mcp(None, org, "")
    assert "error" in fetch_for_mcp(None, org, "client:")
    assert fetch_for_mcp(None, org, "widget:123") == {"error": "unknown id type 'widget'"}


def test_search_empty_query_returns_no_results_without_db():
    assert search_for_mcp(None, uuid.uuid4(), "   ") == {"results": []}
