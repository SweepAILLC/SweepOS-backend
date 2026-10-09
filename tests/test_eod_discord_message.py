"""EOD form Discord post: setter context + content attracting ICP in the body, numbers as fields."""
from datetime import date

from app.api.kpi import build_eod_discord_message


def test_context_and_content_in_description_numbers_as_fields():
    msg = build_eod_discord_message(
        "Sam",
        date(2026, 9, 27),
        {
            "followups_sent": 11,
            "respondents": 3,
            "cash_collected": 1500,
            "content_posted": True,
            "best_content_type": "Reels — transformation stories",
            "setter_context": "Warm leads from the webinar.\nTwo asked about payment plans.",
            "setter_booked_client_ids": ["a", "b"],
        },
    )
    assert msg["title"] == "EOD form submitted — 2026-09-27"
    d = msg["description"]
    assert "Submitted by: **Sam**" in d
    assert "**Content attracting ICP:** Reels — transformation stories" in d
    assert "> Warm leads from the webinar.\n> Two asked about payment plans." in d
    fields = dict(msg["fields"])
    assert fields == {
        "Replies": "3",
        "Follow-ups sent": "11",
        "Cash collected": "$1,500",
        "Content posted": "Yes",
        "Booked clients tagged": "2",
    }
    assert "setter_context" not in str(msg["fields"])  # text isn't duplicated as a raw field


def test_long_context_is_capped_and_empty_notes_omitted():
    msg = build_eod_discord_message("Sam", date(2026, 9, 27), {"setter_context": "x" * 5000, "best_content_type": "  "})
    assert len(msg["description"]) <= 3901 and msg["description"].endswith("…")
    assert "Content attracting ICP" not in msg["description"]
