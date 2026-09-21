"""GHL client payload normalization (pure functions, no network)."""
from app.services.ghl_client import normalize_ghl_appointment_event, normalize_ghl_contact


class TestNormalizeGhlContact:
    def test_full_contact(self):
        raw = {
            "id": "abc123",
            "email": "  jane@example.com  ",
            "phone": "+15551234567",
            "firstName": "Jane",
            "lastName": "Doe",
        }
        out = normalize_ghl_contact(raw)
        assert out == {
            "ghl_contact_id": "abc123",
            "email": "jane@example.com",
            "phone": "+15551234567",
            "first_name": "Jane",
            "last_name": "Doe",
        }

    def test_missing_fields_become_none(self):
        out = normalize_ghl_contact({"id": "abc123"})
        assert out["email"] is None
        assert out["phone"] is None
        assert out["first_name"] is None
        assert out["last_name"] is None
        assert out["ghl_contact_id"] == "abc123"

    def test_blank_strings_become_none(self):
        out = normalize_ghl_contact({"id": "x", "email": "   ", "firstName": ""})
        assert out["email"] is None
        assert out["first_name"] is None

    def test_no_id_becomes_none(self):
        out = normalize_ghl_contact({"email": "a@b.com"})
        assert out["ghl_contact_id"] is None


class TestNormalizeGhlAppointmentEvent:
    def test_nested_appointment_create(self):
        payload = {
            "type": "AppointmentCreate",
            "locationId": "loc_1",
            "appointment": {
                "id": "appt_1",
                "calendarId": "cal_1",
                "title": "Discovery Call",
                "startTime": "2026-09-20T15:00:00+00:00",
                "endTime": "2026-09-20T15:30:00+00:00",
                "appointmentStatus": "confirmed",
            },
            "contact": {"email": "lead@example.com", "firstName": "Jane", "lastName": "Doe"},
        }
        out = normalize_ghl_appointment_event(payload)
        assert out["event_id"] == "appt_1"
        assert out["calendar_id"] == "cal_1"
        assert out["attendee_email"] == "lead@example.com"
        assert out["attendee_name"] == "Jane Doe"
        assert out["cancelled"] is False

    def test_cancelled_status_flagged(self):
        payload = {
            "type": "AppointmentUpdate",
            "appointment": {"id": "appt_1", "calendarId": "cal_1", "appointmentStatus": "cancelled"},
        }
        out = normalize_ghl_appointment_event(payload)
        assert out["cancelled"] is True

    def test_delete_event_type_flagged_cancelled(self):
        payload = {
            "type": "AppointmentDelete",
            "appointment": {"id": "appt_1", "calendarId": "cal_1", "appointmentStatus": "confirmed"},
        }
        out = normalize_ghl_appointment_event(payload)
        assert out["cancelled"] is True

    def test_flat_workflow_forwarded_shape(self):
        payload = {
            "id": "appt_2",
            "calendarId": "cal_2",
            "startTime": "2026-09-20T15:00:00+00:00",
            "email": "lead2@example.com",
        }
        out = normalize_ghl_appointment_event(payload)
        assert out["event_id"] == "appt_2"
        assert out["calendar_id"] == "cal_2"
        assert out["attendee_email"] == "lead2@example.com"

    def test_missing_calendar_id_returns_none(self):
        payload = {"appointment": {"id": "appt_1"}}
        assert normalize_ghl_appointment_event(payload) is None

    def test_missing_event_id_returns_none(self):
        payload = {"appointment": {"calendarId": "cal_1"}}
        assert normalize_ghl_appointment_event(payload) is None

    def test_no_contact_email_is_none_not_error(self):
        payload = {"appointment": {"id": "appt_1", "calendarId": "cal_1"}}
        out = normalize_ghl_appointment_event(payload)
        assert out["attendee_email"] is None
        assert out["attendee_name"] is None
