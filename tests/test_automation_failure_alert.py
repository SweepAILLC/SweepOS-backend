"""emit_automation_failure_discord: pipeline-automation failures are no longer
silently swallowed by print()/logger.warning alone."""
import uuid
from unittest.mock import patch

from app.services.discord_notify import EVENT_TYPES, is_event_type_known
from app.services.integration_side_effects import emit_automation_failure_discord


def test_automation_failure_is_a_registered_event_type():
    assert is_event_type_known("automation_failure")
    assert any(e["key"] == "automation_failure" for e in EVENT_TYPES)


def test_emit_sends_discord_event_with_error_and_client_fields():
    org_id = uuid.uuid4()
    client_id = uuid.uuid4()
    with patch(
        "app.services.discord_notify.send_discord_event_background"
    ) as send:
        emit_automation_failure_discord(
            org_id=org_id,
            where="test.some_function",
            error=ValueError("boom"),
            client_id=client_id,
        )
    send.assert_called_once()
    args, kwargs = send.call_args
    assert args[0] == org_id
    assert args[1] == "automation_failure"
    assert kwargs["title"] == "Pipeline automation error"
    field_names = [f[0] for f in kwargs["fields"]]
    assert "Where" in field_names
    assert "Client" in field_names
    assert "Error" in field_names


def test_emit_never_raises_when_discord_send_fails():
    with patch(
        "app.services.discord_notify.send_discord_event_background",
        side_effect=RuntimeError("discord down"),
    ):
        emit_automation_failure_discord(
            org_id=uuid.uuid4(), where="test.fn", error=ValueError("boom")
        )
