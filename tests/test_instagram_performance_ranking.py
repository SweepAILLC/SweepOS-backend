"""Exposure-weighted ranking for Instagram top/underperforming posts.

Raw engagement_rate_pct (interactions / reach) is noisy at low reach — a post with 3
views and 3 likes is a 100% rate but tells us nothing. These tests pin down that the
Bayesian-shrunk score used for ranking keeps tiny-exposure posts from dominating either
end of the leaderboard, while well-exposed posts keep close to their actual rate.
"""
from types import SimpleNamespace

from app.services.instagram_performance import (
    MIN_EXPOSURE,
    _exposure,
    _exposure_prior,
    _rank_score,
)


def _media(reach=None, views=None, engagement_rate_pct=None):
    return SimpleNamespace(reach=reach, views=views, engagement_rate_pct=engagement_rate_pct)


def test_exposure_prefers_reach_then_falls_back_to_views():
    assert _exposure(_media(reach=100, views=5000)) == 100
    assert _exposure(_media(reach=None, views=5000)) == 5000
    assert _exposure(_media(reach=0, views=5000)) == 5000
    assert _exposure(_media(reach=None, views=None)) is None


def test_tiny_exposure_post_is_excluded_from_ranking_entirely():
    """3 views / 3 likes -> 100% raw rate, but below MIN_EXPOSURE -> no score at all."""
    noisy = _media(reach=3, engagement_rate_pct=100.0)
    assert _exposure(noisy) == 3
    prior_rate, prior_weight = _exposure_prior(
        [noisy, _media(reach=5000, engagement_rate_pct=10.0)]
    )
    score = _rank_score(
        noisy.engagement_rate_pct, _exposure(noisy), prior_rate_pct=prior_rate, prior_weight=prior_weight
    )
    assert score is None


def test_low_exposure_high_rate_post_is_shrunk_below_a_well_exposed_lower_rate_post():
    """A borderline-reach post with a freak-high rate must not outrank a well-exposed,
    solidly-engaged post just because its raw rate is higher."""
    # A normal spread of well-exposed posts sets a realistic account prior (~8%, ~5000 reach).
    normal = [
        _media(reach=5000, engagement_rate_pct=10.0),
        _media(reach=4000, engagement_rate_pct=6.25),
        _media(reach=6000, engagement_rate_pct=8.0),
        _media(reach=4500, engagement_rate_pct=7.5),
        _media(reach=5500, engagement_rate_pct=9.0),
    ]
    solid = normal[0]
    borderline = _media(reach=60, engagement_rate_pct=66.7)  # just above MIN_EXPOSURE
    big_low = _media(reach=8000, engagement_rate_pct=0.75)

    posts = normal + [borderline, big_low]
    prior_rate, prior_weight = _exposure_prior(posts)

    scores = {
        id(p): _rank_score(
            p.engagement_rate_pct, _exposure(p), prior_rate_pct=prior_rate, prior_weight=prior_weight
        )
        for p in posts
    }

    assert scores[id(solid)] > scores[id(borderline)]
    assert scores[id(borderline)] > scores[id(big_low)]
    # Shrinkage pulls the borderline post's score far below its own raw rate.
    assert scores[id(borderline)] < borderline.engagement_rate_pct / 2
    assert borderline.engagement_rate_pct > solid.engagement_rate_pct  # raw rate alone would rank it #1


def test_well_exposed_post_keeps_close_to_its_raw_rate():
    posts = [
        _media(reach=5000, engagement_rate_pct=10.0),
        _media(reach=4000, engagement_rate_pct=6.25),
        _media(reach=8000, engagement_rate_pct=0.75),
    ]
    prior_rate, prior_weight = _exposure_prior(posts)
    p = posts[0]
    score = _rank_score(
        p.engagement_rate_pct, _exposure(p), prior_rate_pct=prior_rate, prior_weight=prior_weight
    )
    assert abs(score - p.engagement_rate_pct) < 2.0


def test_exposure_prior_floors_weight_at_reach_floor_for_low_activity_accounts():
    from app.services.instagram_performance import REACH_FLOOR

    tiny_account_posts = [_media(reach=20, engagement_rate_pct=5.0)]
    _, prior_weight = _exposure_prior(tiny_account_posts)
    assert prior_weight == REACH_FLOOR


def test_min_exposure_constant_is_positive_and_below_reach_floor():
    from app.services.instagram_performance import REACH_FLOOR

    assert 0 < MIN_EXPOSURE < REACH_FLOOR
