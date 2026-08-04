"""
Rules-based Instagram performance analytics for Marketing Intel.

Pure reads over instagram_media / instagram_account_snapshots — never hits Composio.
"""
from __future__ import annotations

import statistics
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session

from app.models.instagram_account_snapshot import InstagramAccountSnapshot
from app.models.instagram_media import InstagramMedia
from app.models.oauth_token import OAuthProvider, OAuthToken
from app.services.instagram_sync_service import (
    caption_len_bucket,
    resolve_capabilities,
)

MIN_SAMPLE = 3
REACH_FLOOR = 50  # ignore tiny-sample posts when ranking

# Human labels for Performance UI / verdicts (avoid raw enum keys).
_DIMENSION_LABELS = {
    "format_bucket": "format",
    "hook_pattern": "opening hook style",
    "funnel_stage": "funnel stage",
    "posted_dow": "day posted",
    "caption_len": "caption length",
    "theme_keys": "theme",
}

_FORMAT_LABELS = {
    "reel": "Reels",
    "reels": "Reels",
    "carousel": "carousels",
    "image": "single-image posts",
    "video": "feed videos",
    "story": "Stories",
}

_HOOK_LABELS = {
    "question": "question hooks (open with a question)",
    "listicle_number": "number / list hooks (e.g. “3 ways…”)",
    "howto": "how-to hooks",
    "story": "story / anecdote hooks",
    "contrarian": "contrarian / myth-busting hooks",
    "cta": "CTA-first hooks",
    "short_punch": "short punchy hooks (≤40 chars)",
    "statement": "statement hooks",
    "none": "posts with weak or missing hooks",
}

_FUNNEL_LABELS = {
    "TOF": "top-of-funnel (TOF) awareness posts",
    "MOF": "middle-of-funnel (MOF) trust / education posts",
    "BOF": "bottom-of-funnel (BOF) offer / CTA posts",
}

_DOW_LABELS = {
    "Mon": "Mondays",
    "Tue": "Tuesdays",
    "Wed": "Wednesdays",
    "Thu": "Thursdays",
    "Fri": "Fridays",
    "Sat": "Saturdays",
    "Sun": "Sundays",
}

_CAPTION_LEN_LABELS = {
    "short": "short captions",
    "medium": "medium captions",
    "long": "long captions",
}


def _dimension_label(dim: str) -> str:
    return _DIMENSION_LABELS.get(dim, dim.replace("_", " "))


def _value_label(dim: str, val: str) -> str:
    v = (val or "").strip()
    if dim == "format_bucket":
        return _FORMAT_LABELS.get(v.lower(), v)
    if dim == "hook_pattern":
        return _HOOK_LABELS.get(v, v.replace("_", " "))
    if dim == "funnel_stage":
        return _FUNNEL_LABELS.get(v.upper(), v)
    if dim == "posted_dow":
        return _DOW_LABELS.get(v, v)
    if dim == "caption_len":
        return _CAPTION_LEN_LABELS.get(v.lower(), v)
    if dim == "theme_keys":
        return f"“{v}” themed posts"
    return v.replace("_", " ")


def _plain_insight(
    *,
    dim: str,
    val: str,
    lift: float,
    n: int,
    verdict: str,
    example_hooks: Optional[Sequence[str]] = None,
) -> str:
    """One plain-English line for What to do next / What's working."""
    label = _value_label(dim, val)
    based = f"based on {n} post{'s' if n != 1 else ''}"
    examples = ""
    hooks = [h for h in (example_hooks or []) if h][:2]
    if hooks:
        quoted = "; ".join(f"“{h[:80]}”" for h in hooks)
        examples = f" Examples that worked: {quoted}."

    if dim == "funnel_stage" and val.upper() == "TOF":
        if verdict == "double_down":
            return (
                f"Your top-of-funnel (TOF / awareness) posts are beating your average by "
                f"{lift:.0f}% engagement ({based}). Double down with more TOF content like these.{examples}"
            )
        if verdict == "keep":
            return (
                f"Your top-of-funnel (TOF / awareness) posts are around your average ({based}). "
                "Keep posting TOF, but test new hooks to find a clearer winner."
            )
        return (
            f"Your top-of-funnel (TOF / awareness) posts are underperforming by "
            f"{abs(lift):.0f}% ({based}). Rewrite awareness hooks before posting more TOF."
        )

    if dim == "funnel_stage":
        stage = _FUNNEL_LABELS.get(val.upper(), label)
        if verdict == "double_down":
            return (
                f"{stage[0].upper() + stage[1:]} are beating your average by {lift:.0f}% "
                f"({based}). Double down on this stage next.{examples}"
            )
        if verdict == "keep":
            return (
                f"{stage[0].upper() + stage[1:]} are roughly average ({based}). "
                "Keep this stage in rotation while testing variations."
            )
        return (
            f"{stage[0].upper() + stage[1:]} are underperforming by {abs(lift):.0f}% "
            f"({based}). Pause or rewrite this stage."
        )

    if dim == "format_bucket":
        if verdict == "double_down":
            return f"{label[0].upper() + label[1:]} outperform your other formats by {lift:.0f}% ({based}). Double down by posting more of these."
        if verdict == "keep":
            return f"{label[0].upper() + label[1:]} are close to average ({based}). Keep using while testing alternatives."
        return f"{label[0].upper() + label[1:]} underperform by {abs(lift):.0f}% ({based}). Use less of this format."

    if dim == "hook_pattern":
        if verdict == "double_down":
            return f"{label[0].upper() + label[1:]} beat your average by {lift:.0f}% ({based}). Double down on this opening style.{examples}"
        if verdict == "keep":
            return f"{label[0].upper() + label[1:]} are about average ({based}). Keep testing this opening style."
        return f"{label[0].upper() + label[1:]} lag by {abs(lift):.0f}% ({based}). Avoid this opening style."

    if dim == "posted_dow":
        if verdict == "double_down":
            return f"Posts published on {label} engage {lift:.0f}% better than your average ({based}). Double down by prioritizing this day."
        return f"Posts published on {label} engage {abs(lift):.0f}% worse ({based}). Avoid this day when you can."

    if dim == "caption_len":
        if verdict == "double_down":
            return f"{label[0].upper() + label[1:]} outperform by {lift:.0f}% ({based}). Double down in this length range."
        return f"{label[0].upper() + label[1:]} underperform by {abs(lift):.0f}% ({based}). Try a different length."

    if verdict == "double_down":
        return f"{label[0].upper() + label[1:]} beat your average by {lift:.0f}% ({based}). Double down."
    return f"{label[0].upper() + label[1:]} lag by {abs(lift):.0f}% ({based}). Pause or rewrite."


def _median(vals: Sequence[float]) -> Optional[float]:
    clean = [float(v) for v in vals if v is not None]
    if not clean:
        return None
    return float(statistics.median(clean))



def _pct_delta(cur: Optional[float], prev: Optional[float]) -> Optional[float]:
    if cur is None or prev is None or prev == 0:
        return None
    return round(((cur - prev) / abs(prev)) * 100.0, 1)


def _post_card(m: InstagramMedia) -> Dict[str, Any]:
    return {
        "ig_media_id": m.ig_media_id,
        "permalink": m.permalink,
        "thumbnail_url": m.thumbnail_url,
        "caption": (m.caption or "")[:280],
        "hook_text": m.hook_text,
        "hook_pattern": m.hook_pattern,
        "format_bucket": m.format_bucket,
        "funnel_stage": m.funnel_stage,
        "theme_keys": m.theme_keys or [],
        "posted_at": m.posted_at.isoformat() if m.posted_at else None,
        "views": m.views,
        "reach": m.reach,
        "saved": m.saved,
        "likes": m.likes,
        "comments": m.comments,
        "shares": m.shares,
        "total_interactions": m.total_interactions,
        "engagement_rate_pct": m.engagement_rate_pct,
        "save_rate_pct": m.save_rate_pct,
        "insights_status": m.insights_status,
        "metrics_settled": m.metrics_settled,
        "linked_concept_id": m.linked_concept_id,
        "avg_watch_time_sec": m.avg_watch_time_sec,
    }


def _dimension_value(m: InstagramMedia, dimension: str) -> Optional[str]:
    if dimension == "format_bucket":
        return m.format_bucket
    if dimension == "hook_pattern":
        return m.hook_pattern
    if dimension == "funnel_stage":
        return m.funnel_stage
    if dimension == "posted_dow":
        if m.posted_dow is None:
            return None
        names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        return names[int(m.posted_dow) % 7]
    if dimension == "caption_len":
        return caption_len_bucket(m.caption_len)
    if dimension == "theme_keys":
        return None  # handled separately (multi-value)
    return None


def _what_works(posts: Sequence[InstagramMedia]) -> List[Dict[str, Any]]:
    """
    Build actionable performance calls.

    Always returns hook/awareness(format/funnel) options when data exists, even when
    sample size is thin. Low-sample rows are marked confidence='low'.
    """
    eng_all = [
        float(p.engagement_rate_pct)
        for p in posts
        if p.engagement_rate_pct is not None
        and (p.reach is None or p.reach >= REACH_FLOOR or p.insights_status == "unavailable")
    ]
    org_med = _median(eng_all)
    if org_med is None or org_med <= 0:
        org_med = 1.0

    target_dims = ("hook_pattern", "funnel_stage", "format_bucket")
    buckets: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    hook_examples: Dict[Tuple[str, str], List[Tuple[float, str]]] = defaultdict(list)

    for p in posts:
        if p.engagement_rate_pct is None:
            continue
        rate = float(p.engagement_rate_pct)
        hook = (p.hook_text or "").strip()
        for dim in target_dims:
            val = _dimension_value(p, dim)
            if not val:
                continue
            key = (dim, val)
            buckets[key].append(rate)
            if hook:
                hook_examples[key].append((rate, hook[:120]))

    scored: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for (dim, val), rates in buckets.items():
        if not rates:
            continue
        med = _median(rates)
        if med is None:
            continue
        lift = round(((med - org_med) / org_med) * 100.0, 1) if org_med else 0.0
        n = len(rates)
        confidence = "high" if n >= MIN_SAMPLE else "low"

        # Always produce a directional action for low-sample buckets.
        if confidence == "low":
            verdict = "double_down" if lift >= 0 else "stop"
        else:
            if lift >= 20:
                verdict = "double_down"
            elif lift <= -20:
                verdict = "stop"
            else:
                verdict = "keep"

        examples = [
            h
            for _, h in sorted(hook_examples.get((dim, val), []), key=lambda t: -t[0])[:3]
        ]
        seen = set()
        uniq_examples: List[str] = []
        for h in examples:
            if h not in seen:
                seen.add(h)
                uniq_examples.append(h)

        summary = _plain_insight(
            dim=dim,
            val=val,
            lift=lift,
            n=n,
            verdict=verdict,
            example_hooks=uniq_examples if verdict == "double_down" else None,
        )
        scored[dim].append(
            {
                "dimension": dim,
                "dimension_label": _dimension_label(dim),
                "value": val,
                "value_label": _value_label(dim, val),
                "n": n,
                "median_engagement_rate": round(med, 3),
                "org_median_engagement_rate": round(org_med, 3),
                "lift_vs_median_pct": lift,
                "verdict": verdict,
                "confidence": confidence,
                "summary": summary,
                "example_hooks": uniq_examples[:2],
            }
        )

    out: List[Dict[str, Any]] = []
    for dim in target_dims:
        rows = scored.get(dim) or []
        if not rows:
            continue
        rows.sort(key=lambda r: float(r.get("lift_vs_median_pct") or 0), reverse=True)
        best = rows[0]
        worst = rows[-1]
        out.append(best)
        if worst["value"] != best["value"]:
            out.append(worst)

    dim_rank = {"funnel_stage": 0, "format_bucket": 1, "hook_pattern": 2}
    out.sort(
        key=lambda r: (
            0 if r["verdict"] == "double_down" else 1 if r["verdict"] == "stop" else 2,
            dim_rank.get(str(r["dimension"]), 9),
            0 if str(r.get("value") or "").upper() == "TOF" else 1,
            -abs(float(r["lift_vs_median_pct"])),
        )
    )
    return out


def _weekly_trend(posts: Sequence[InstagramMedia], days: int) -> List[Dict[str, Any]]:
    if not posts:
        return []
    # Bucket by ISO week start (Monday)
    buckets: Dict[date, Dict[str, Any]] = {}
    for p in posts:
        if not p.posted_at:
            continue
        d = p.posted_at.date()
        week_start = d - timedelta(days=d.weekday())
        b = buckets.setdefault(
            week_start,
            {
                "week_start": week_start.isoformat(),
                "reach": 0,
                "saved": 0,
                "views": 0,
                "posts": 0,
                "eng_rates": [],
            },
        )
        b["posts"] += 1
        b["reach"] += int(p.reach or 0)
        b["saved"] += int(p.saved or 0)
        b["views"] += int(p.views or 0)
        if p.engagement_rate_pct is not None:
            b["eng_rates"].append(float(p.engagement_rate_pct))

    series = []
    for ws in sorted(buckets.keys()):
        b = buckets[ws]
        series.append(
            {
                "week_start": b["week_start"],
                "reach": b["reach"],
                "saved": b["saved"],
                "views": b["views"],
                "posting_volume": b["posts"],
                "engagement_rate_pct": _median(b["eng_rates"]),
            }
        )
    return series


def _trend_flags(trend: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flags: List[Dict[str, Any]] = []
    if len(trend) < 3:
        return flags
    for metric, label in (
        ("engagement_rate_pct", "engagement rate"),
        ("reach", "reach"),
        ("saved", "saves"),
    ):
        vals = []
        labels = []
        for row in trend:
            v = row.get(metric)
            if v is None:
                continue
            vals.append(float(v))
            labels.append(row["week_start"])
        if len(vals) < 3:
            continue
        last3 = vals[-3:]
        last3_labels = labels[-3:]
        if not (last3[0] > last3[1] > last3[2]):
            continue
        drop = ((last3[0] - last3[2]) / last3[0]) * 100.0 if last3[0] else 0
        if drop < 10:
            continue
        flags.append(
            {
                "id": f"ig-week-trend-{metric}",
                "severity": "high" if drop >= 25 else "medium",
                "title": f"{label.title()} declining for 3 weeks",
                "detail": (
                    f"{label} fell {drop:.0f}% from {last3_labels[0]} → {last3_labels[2]} "
                    f"({last3[0]:.1f} → {last3[2]:.1f}). Review recent hooks and formats."
                ),
                "metric": metric,
                "drop_pct": round(drop, 1),
            }
        )
    return flags


def _verdicts(what_works: Sequence[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []

    # Always return one clear action per dimension (awareness, format, hook),
    # even when the signal is weak.
    preferred_dims = ("funnel_stage", "format_bucket", "hook_pattern")

    for dim in preferred_dims:
        dim_rows = [r for r in what_works if str(r.get("dimension")) == dim]
        if not dim_rows:
            continue
        # Always lead with the strongest upside row for positive "double down" guidance.
        ranked = sorted(
            dim_rows,
            key=lambda r: float(r.get("lift_vs_median_pct") or -9999),
            reverse=True,
        )
        row = ranked[0]
        lift = float(row.get("lift_vs_median_pct") or 0)
        if lift >= 0:
            summary = row.get("summary") or _plain_insight(
                dim=str(row["dimension"]),
                val=str(row["value"]),
                lift=lift,
                n=int(row["n"]),
                verdict="double_down",
                example_hooks=row.get("example_hooks"),
            )
        else:
            label = str(row.get("value_label") or row.get("value") or "this pattern")
            n = int(row.get("n") or 0)
            based = f"based on {n} post{'s' if n != 1 else ''}"
            summary = (
                f"Best near-term bet: test more {label} and iterate quickly; it is currently your strongest "
                f"option in this dimension ({based})."
            )
        if row.get("confidence") == "low":
            summary = f"{summary} (Early signal: low sample size.)"
        lines.append(str(summary))

    if len(lines) < 5:
        for row in what_works:
            summary = _plain_insight(
                dim=str(row["dimension"]),
                val=str(row["value"]),
                lift=float(row.get("lift_vs_median_pct") or 0),
                n=int(row["n"]),
                verdict="double_down" if float(row.get("lift_vs_median_pct") or 0) >= 0 else "keep",
                example_hooks=row.get("example_hooks"),
            )
            if row.get("confidence") == "low":
                summary = f"{summary} (Early signal: low sample size.)"
            if summary in lines:
                continue
            lines.append(str(summary))
            if len(lines) >= 5:
                break
    return lines


def build_instagram_performance(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
) -> Dict[str, Any]:
    days = max(7, min(int(days or 90), 365))
    token = (
        db.query(OAuthToken)
        .filter(
            OAuthToken.org_id == org_id,
            OAuthToken.provider == OAuthProvider.INSTAGRAM,
        )
        .first()
    )
    connected = token is not None
    if not connected:
        return {
            "connected": False,
            "org_id": str(org_id),
            "days": days,
            "summary": None,
            "trend": [],
            "top_posts": [],
            "bottom_posts": [],
            "what_works": [],
            "verdicts": [],
            "flags": [],
            "capabilities": {"insights": False, "reason": "Instagram is not connected."},
            "unsettled_post_count": 0,
            "last_synced_at": None,
            "usage": "Connect Instagram in Integrations to pull content performance.",
        }

    now = datetime.utcnow()
    range_end = now
    range_start = now - timedelta(days=days)
    prev_start = range_start - timedelta(days=days)

    posts = (
        db.query(InstagramMedia)
        .filter(
            InstagramMedia.org_id == org_id,
            InstagramMedia.posted_at >= range_start,
            InstagramMedia.posted_at <= range_end,
        )
        .order_by(InstagramMedia.posted_at.desc())
        .all()
    )
    prev_posts = (
        db.query(InstagramMedia)
        .filter(
            InstagramMedia.org_id == org_id,
            InstagramMedia.posted_at >= prev_start,
            InstagramMedia.posted_at < range_start,
        )
        .all()
    )

    def _sum_field(rows: Sequence[InstagramMedia], field: str) -> int:
        total = 0
        for r in rows:
            v = getattr(r, field, None)
            if isinstance(v, (int, float)):
                total += int(v)
        return total

    def _avg_eng(rows: Sequence[InstagramMedia]) -> Optional[float]:
        return _median(
            [float(r.engagement_rate_pct) for r in rows if r.engagement_rate_pct is not None]
        )

    cur_reach = _sum_field(posts, "reach")
    prev_reach = _sum_field(prev_posts, "reach")
    cur_views = _sum_field(posts, "views")
    prev_views = _sum_field(prev_posts, "views")
    cur_saved = _sum_field(posts, "saved")
    prev_saved = _sum_field(prev_posts, "saved")
    cur_eng = _avg_eng(posts)
    prev_eng = _avg_eng(prev_posts)

    # Follower growth from snapshots
    snaps = (
        db.query(InstagramAccountSnapshot)
        .filter(
            InstagramAccountSnapshot.org_id == org_id,
            InstagramAccountSnapshot.snapshot_date >= range_start.date(),
        )
        .order_by(InstagramAccountSnapshot.snapshot_date.asc())
        .all()
    )
    followers_start = snaps[0].followers_count if snaps else None
    followers_end = snaps[-1].followers_count if snaps else None
    if followers_start is None or followers_end is None:
        # Fall back to latest snapshot ever
        latest = (
            db.query(InstagramAccountSnapshot)
            .filter(InstagramAccountSnapshot.org_id == org_id)
            .order_by(InstagramAccountSnapshot.snapshot_date.desc())
            .first()
        )
        followers_end = latest.followers_count if latest else None

    follower_growth = None
    if followers_start is not None and followers_end is not None:
        follower_growth = int(followers_end) - int(followers_start)

    summary = {
        "posts": len(posts),
        "reach": cur_reach,
        "reach_delta_pct": _pct_delta(float(cur_reach), float(prev_reach)) if prev_reach else None,
        "views": cur_views,
        "views_delta_pct": _pct_delta(float(cur_views), float(prev_views)) if prev_views else None,
        "saved": cur_saved,
        "saved_delta_pct": _pct_delta(float(cur_saved), float(prev_saved)) if prev_saved else None,
        "engagement_rate_pct": cur_eng,
        "engagement_rate_delta_pct": _pct_delta(cur_eng, prev_eng),
        "followers_count": followers_end,
        "follower_growth": follower_growth,
        "prev_period_posts": len(prev_posts),
    }

    # Rank posts
    rankable = [
        p
        for p in posts
        if p.engagement_rate_pct is not None
        and (p.reach is None or p.reach >= REACH_FLOOR or p.insights_status != "ok")
    ]
    rankable.sort(key=lambda p: float(p.engagement_rate_pct or 0), reverse=True)
    top = [_post_card(p) for p in rankable[:8]]
    bottom = [_post_card(p) for p in reversed(rankable[-5:])] if len(rankable) >= 5 else []

    what = _what_works(posts)
    trend = _weekly_trend(posts, days)
    flags = _trend_flags(trend)
    verdicts = _verdicts(what)

    unsettled = sum(1 for p in posts if not p.metrics_settled)
    insights_ok = sum(1 for p in posts if p.insights_status in ("ok", "partial"))
    caps = resolve_capabilities(
        followers_count=followers_end,
        insights_ok_count=insights_ok,
        insights_attempted=len(posts),
    )

    return {
        "connected": True,
        "org_id": str(org_id),
        "days": days,
        "range_start": range_start.date().isoformat(),
        "range_end": range_end.date().isoformat(),
        "summary": summary,
        "trend": trend,
        "top_posts": top,
        "bottom_posts": bottom,
        "what_works": what,
        "verdicts": verdicts,
        "flags": flags,
        "capabilities": caps,
        "unsettled_post_count": unsettled,
        "last_synced_at": token.last_sync_at.isoformat() if token.last_sync_at else None,
        "username": None,  # filled by API from user_info cache / token scope if needed
        "usage": (
            "verdicts are the tactical calls. what_works shows dimensional lift vs your "
            "median engagement. Even low-sample rows are shown as early signals. Prefer verdict=double_down; "
            "avoid stop. top_posts/bottom_posts are ranked by engagement rate with a "
            "reach floor to reduce small-sample noise."
        ),
    }


def performance_fingerprint(db: Session, org_id: uuid.UUID) -> str:
    """Compact fingerprint so content-studio regen fires when winners shift."""
    import hashlib
    import json

    payload = build_instagram_performance(db, org_id, days=90)
    if not payload.get("connected"):
        return "ig:none"
    winners = [
        f"{w['dimension']}:{w['value']}:{w['verdict']}"
        for w in (payload.get("what_works") or [])[:12]
    ]
    blob = {
        "posts": (payload.get("summary") or {}).get("posts"),
        "winners": winners,
        "last_sync": payload.get("last_synced_at"),
    }
    return "ig:" + hashlib.sha256(json.dumps(blob, sort_keys=True).encode()).hexdigest()[:16]


def top_posts_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    *,
    days: int = 90,
    format_bucket: Optional[str] = None,
    theme_key: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    perf = build_instagram_performance(db, org_id, days=days)
    posts = list(perf.get("top_posts") or [])
    if format_bucket:
        fb = format_bucket.lower().strip()
        posts = [p for p in posts if str(p.get("format_bucket") or "").lower() == fb]
    if theme_key:
        tk = theme_key.strip()
        posts = [p for p in posts if tk in (p.get("theme_keys") or [])]
    return {
        "org_id": str(org_id),
        "count": len(posts[:limit]),
        "posts": posts[:limit],
        "capabilities": perf.get("capabilities"),
    }
