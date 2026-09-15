"""Instagram Meta session-invalid (OAuthException 190) classification."""
import pytest

from app.services.composio_client import (
    AUTH_INVALID_MARKER,
    ComposioAuthError,
    ComposioToolError,
    _is_auth_invalid_message,
    _unwrap_tool_result,
    instagram_auth_invalid_from_scope,
    instagram_username_from_scope,
)

PROD_190 = (
    'Failed to get user info (status 401). Response: {"error":{"message":'
    '"Error validating access token: The session has been invalidated because '
    "the user changed their password or Facebook has changed the session for "
    'security reasons.","type":"OAuthException","code":190,"error_subcode":0}}'
)


def test_classifier_matches_prod_190():
    assert _is_auth_invalid_message(PROD_190)


def test_classifier_ignores_permission_and_generic_errors():
    assert not _is_auth_invalid_message(
        "(#10) Application does not have permission for this action"
    )
    assert not _is_auth_invalid_message("unauthorized")
    assert not _is_auth_invalid_message("composio timeout")


def test_unwrap_maps_190_to_auth_error():
    with pytest.raises(ComposioAuthError):
        _unwrap_tool_result(
            {"successful": False, "error": PROD_190},
            slug="INSTAGRAM_GET_USER_INFO",
        )


def test_unwrap_leaves_other_failures_as_tool_error():
    with pytest.raises(ComposioToolError) as ei:
        _unwrap_tool_result(
            {"successful": False, "error": "(#10) Application does not have permission"},
            slug="INSTAGRAM_GET_USER_INFO",
        )
    assert not isinstance(ei.value, ComposioAuthError)


def test_username_parse_ignores_auth_invalid_marker():
    assert instagram_username_from_scope("instagram:k.ai.lb") == "k.ai.lb"
    assert (
        instagram_username_from_scope(f"instagram:k.ai.lb {AUTH_INVALID_MARKER}")
        == "k.ai.lb"
    )
    assert instagram_auth_invalid_from_scope("instagram:k.ai.lb") is False
    assert instagram_auth_invalid_from_scope(
        f"instagram:k.ai.lb {AUTH_INVALID_MARKER}"
    )
