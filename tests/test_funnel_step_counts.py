"""Per-step funnel analytics: raw event counts plus unique-visitor drop-off."""
from types import SimpleNamespace

from app.api.funnels import build_step_counts


def _step(order, event_name, label=None):
    return SimpleNamespace(step_order=order, event_name=event_name, label=label)


STEPS = [
    _step(1, "page_view", "Landing"),
    _step(2, "vsl_play", "Watched VSL"),
    _step(3, "form_submit", "Applied"),
]


def test_counts_and_rates_from_previous_step():
    rows = build_step_counts(
        STEPS,
        {"page_view": (500, 400), "vsl_play": (300, 200), "form_submit": (60, 50)},
    )
    assert [r.count for r in rows] == [500, 300, 60]
    assert [r.unique_visitors for r in rows] == [400, 200, 50]
    assert rows[0].conversion_rate is None and rows[0].unique_conversion_rate is None
    assert rows[1].conversion_rate == 60.0
    assert rows[1].unique_conversion_rate == 50.0
    assert rows[2].unique_conversion_rate == 25.0


def test_step_with_no_events_is_zero_not_missing():
    rows = build_step_counts(STEPS, {"page_view": (10, 8)})
    assert [r.count for r in rows] == [10, 0, 0]
    assert rows[1].unique_conversion_rate == 0.0
    # Previous step had zero visitors: no rate rather than a divide-by-zero.
    assert rows[2].unique_conversion_rate is None
    assert rows[2].conversion_rate is None


def test_steps_sharing_an_event_name_get_the_same_totals():
    steps = [_step(1, "page_view"), _step(2, "page_view")]
    rows = build_step_counts(steps, {"page_view": (40, 30)})
    assert [r.unique_visitors for r in rows] == [30, 30]
    assert rows[1].unique_conversion_rate == 100.0


def test_labels_and_order_pass_through():
    rows = build_step_counts(STEPS, {})
    assert [(r.step_order, r.label, r.event_name) for r in rows] == [
        (1, "Landing", "page_view"),
        (2, "Watched VSL", "vsl_play"),
        (3, "Applied", "form_submit"),
    ]
