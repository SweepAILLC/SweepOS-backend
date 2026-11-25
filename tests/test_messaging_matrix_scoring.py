"""Messaging Matrix scoring: percentiles, shrinkage, paid cost mode."""
from app.services import messaging_matrix_scoring as s


def test_percentiles_spread_and_ties():
    p = s.percentiles({"a": 1.0, "b": 2.0, "c": 3.0})
    assert p == {"a": 0.0, "b": 50.0, "c": 100.0}
    tied = s.percentiles({"a": 1.0, "b": 1.0, "c": 3.0})
    assert tied["a"] == tied["b"] == 25.0


def test_percentiles_lower_is_better_and_single_value():
    assert s.percentiles({"cheap": 10.0, "pricey": 90.0}, higher_is_better=False) == {"cheap": 100.0, "pricey": 0.0}
    assert s.percentiles({"only": 5.0}) == {"only": 50.0}
    assert s.percentiles({}) == {}


def test_shrunk_mean_one_lucky_post_cannot_top_the_matrix():
    one_hit = s.shrunk_mean([100.0])
    steady = s.shrunk_mean([85.0] * 6)
    assert one_hit == 66.67
    assert steady > one_hit
    assert s.shrunk_mean([]) is None


def test_paid_mode_uses_booked_calls_only_with_enough_ads():
    few = {"a": {"booked_calls": 1}, "b": {"booked_calls": 1}, "c": {"booked_calls": 0}}
    assert s.paid_cost_mode(few) == "result"
    enough = {k: {"booked_calls": 1} for k in "abc"}
    assert s.paid_cost_mode(enough) == "booked_call"


def test_paid_item_scores_cheaper_cost_per_call_wins():
    ads = {
        "cheap": {"spend_cents": 10000, "booked_calls": 5},
        "mid": {"spend_cents": 10000, "booked_calls": 2},
        "dud": {"spend_cents": 10000, "booked_calls": 1},
        "zero": {"spend_cents": 20000, "booked_calls": 0},
    }
    scores, mode = s.paid_item_scores(ads)
    assert mode == "booked_call"
    assert scores["cheap"] > scores["mid"] > scores["dud"] > scores["zero"]


def test_paid_item_scores_falls_back_to_results_and_hold_breaks_ties():
    ads = {
        "a": {"spend_cents": 5000, "results": 10, "thruplays": 80, "video_3s_views": 100},
        "b": {"spend_cents": 5000, "results": 10, "thruplays": 20, "video_3s_views": 100},
    }
    scores, mode = s.paid_item_scores(ads)
    assert mode == "result"
    assert scores["a"] > scores["b"]


def test_angle_scores_blend_by_sample_count():
    out = s.angle_scores([90.0, 90.0, 90.0], [10.0])
    assert out["organic_samples"] == 3 and out["paid_samples"] == 1
    assert out["organic_score"] == 74.0
    assert out["paid_score"] == 36.67
    assert out["overall_score"] == round((74.0 * 3 + 36.67) / 4, 2)


def test_angle_scores_single_channel_and_empty():
    assert s.angle_scores([], [80.0])["overall_score"] == s.shrunk_mean([80.0])
    empty = s.angle_scores([], [])
    assert empty["overall_score"] is None and empty["organic_score"] is None
