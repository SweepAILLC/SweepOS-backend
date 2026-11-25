"""Messaging Matrix end to end against a real Postgres (JSONB, lower(name) index).

Runs when TEST_POSTGRES_URL points at a database migrated to head, e.g.
    TEST_POSTGRES_URL=$DATABASE_URL pytest tests/test_messaging_matrix_postgres.py
Everything runs inside one outer transaction that is rolled back; the service's own
commits become savepoint releases. The LLM is faked; no network.
"""
import os
import re
import uuid
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

PG_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="TEST_POSTGRES_URL not set")

OFFER = "Your offer is the bottleneck, not your ads. Fix the offer before you buy more traffic."
FOLLOWERS = "Stop chasing followers. Ten buyers beat ten thousand fans every single month."


@pytest.fixture()
def db():
    engine = create_engine(PG_URL)
    conn = engine.connect()
    outer = conn.begin()
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    try:
        yield session
    finally:
        session.close()
        outer.rollback()
        conn.close()
        engine.dispose()


class FakeLLM:
    """Tags by keyword, reuses angles by reading the numbered list in the prompt."""

    def __init__(self):
        self.calls = {"messaging_angle_tag": 0, "messaging_angle_reasoning": 0, "messaging_angle_recreate": 0}

    def __call__(self, system, prompt, *, feature, prompt_version, **kw):
        assert system.startswith("You are")  # SWEEPBOT_SYSTEM persona
        assert prompt.startswith(f"# version: {prompt_version}")
        self.calls[feature] += 1
        if feature == "messaging_angle_tag":
            body = prompt.lower().split("transcript:")[-1]
            if "offer is the bottleneck" in body:
                name, quote, stage = "Offer is the bottleneck", "Your offer is the bottleneck, not your ads", "problem_aware"
            elif "followers" in body:
                name, quote, stage = "Buyers beat followers", "Ten buyers beat ten thousand fans", "unaware"
            else:
                return {"existing_angle": None, "quote": "", "confidence": 0.2}
            m = re.search(rf"^(\d+)\. {re.escape(name)} \|", prompt, re.M)
            if m:
                return {"existing_angle": int(m.group(1)), "quote": quote, "confidence": 0.9}
            return {
                "existing_angle": None,
                "new_angle": {"name": name, "description": "d", "awareness_stage": stage, "persona": "coaches",
                              "promise": "p", "hook_mechanic": "contrarian"},
                "quote": quote,
                "confidence": 0.9,
            }
        if feature == "messaging_angle_reasoning":
            return {"reasoning": "It names the real constraint.", "confidence": 0.8}
        return {
            "hooks": ["One", "Two", "Three"],
            "outline": ["a", "b", "c", "d"],
            "cta": "Book a call",
            "evidence": ["Your offer is the bottleneck"],
            "confidence": 0.85,
        }


def _seed(db):
    from app.models.instagram_media import InstagramMedia
    from app.models.meta_ad import MetaAd
    from app.models.meta_ad_account import MetaAdAccount
    from app.models.meta_ad_insight_daily import MetaAdInsightDaily
    from app.models.meta_ad_set import MetaAdSet
    from app.models.meta_campaign import MetaCampaign
    from app.models.organization import Organization

    org = Organization(id=uuid.uuid4(), name="ZZ mm org")
    other = Organization(id=uuid.uuid4(), name="ZZ mm other org")
    db.add_all([org, other])
    db.flush()
    now = datetime.utcnow()

    def post(text, reach, comments, transcribed=True):
        return InstagramMedia(
            org_id=org.id, ig_media_id=uuid.uuid4().hex[:12], posted_at=now - timedelta(days=5),
            reach=reach, views=reach, likes=reach // 20, comments=comments, saved=3, shares=2,
            engagement_rate_pct=5.0, permalink="https://instagram.com/p/x",
            transcript=text if transcribed else None, transcript_status="ok" if transcribed else "unavailable",
        )

    posts = [post(OFFER, 5000, 120), post(OFFER + " Again.", 4000, 90), post(FOLLOWERS, 5000, 5), post(None, 3000, 40, False)]
    db.add_all(posts)

    acct = MetaAdAccount(org_id=org.id, meta_ad_account_id="act_1")
    db.add(acct)
    db.flush()
    camp = MetaCampaign(org_id=org.id, ad_account_id=acct.id, meta_campaign_id="c1")
    db.add(camp)
    db.flush()
    aset = MetaAdSet(org_id=org.id, campaign_id=camp.id, meta_adset_id="s1")
    db.add(aset)
    db.flush()

    def ad(mid, text, spend, results):
        row = MetaAd(org_id=org.id, ad_set_id=aset.id, meta_ad_id=mid, name=f"ad {mid}", transcript=text,
                     transcript_status="ok", transcript_source="whisper", transcribed_at=now)
        db.add(row)
        db.add(MetaAdInsightDaily(org_id=org.id, level="ad", entity_id=mid, date=date.today(),
                                  spend_cents=spend, results=results))
        return row

    ads = [ad("a1", OFFER, 20000, 40), ad("a2", FOLLOWERS, 20000, 4), ad("a3", OFFER, 1000, 1)]  # a3 under floor
    db.flush()
    return org, other, posts, ads


def _fake(monkeypatch):
    from app.services import llm_client
    from app.services import messaging_matrix_service as mm

    fake = FakeLLM()
    monkeypatch.setattr(llm_client, "chat_json", fake)
    monkeypatch.setattr(llm_client, "llm_available", lambda: True)
    monkeypatch.setattr(mm, "_redis", lambda: None)
    return fake


def test_refresh_builds_scored_matrix_and_is_idempotent(db, monkeypatch):
    from app.models.messaging_angle import MessagingAngle, MessagingAngleSource
    from app.services import messaging_matrix_service as mm

    fake = _fake(monkeypatch)
    org, other, _posts, _ads = _seed(db)

    stats = mm.refresh_matrix(db, org.id)
    assert "error" not in stats, stats
    assert stats["tagged"] == 5  # 3 transcribed posts + 2 ads over the $50 floor
    assert stats["new_angles"] == 2
    assert fake.calls["messaging_angle_tag"] == 5
    assert fake.calls["messaging_angle_reasoning"] == 2

    out = mm.list_matrix(db, org.id)
    names = [a["name"] for a in out["angles"]]
    assert names == ["Offer is the bottleneck", "Buyers beat followers"]
    offer = out["angles"][0]
    assert offer["organic_samples"] == 2 and offer["paid_samples"] == 1
    assert offer["overall_score"] > out["angles"][1]["overall_score"]
    assert offer["reasoning"] == "It names the real constraint."
    assert out["refresh"]["state"] == "done"

    # Second run: nothing changed, so no LLM calls at all.
    mm.refresh_matrix(db, org.id)
    assert fake.calls["messaging_angle_tag"] == 5
    assert fake.calls["messaging_angle_reasoning"] == 2

    # Detail carries evidence; other orgs cannot read it.
    detail = mm.angle_detail(db, org.id, uuid.UUID(offer["id"]))
    assert len(detail["sources"]) == 3
    assert all(s["quote"] for s in detail["sources"])
    with pytest.raises(LookupError):
        mm.angle_detail(db, other.id, uuid.UUID(offer["id"]))
    assert mm.list_matrix(db, other.id)["angles"] == []

    # Recreate stores an unapproved draft.
    draft = mm.recreate_angle(db, org.id, uuid.UUID(offer["id"]))
    assert draft["approved"] is False and len(draft["hooks"]) == 3
    with pytest.raises(LookupError):
        mm.recreate_angle(db, other.id, uuid.UUID(offer["id"]))

    # Archive hides it from the default list.
    mm.set_angle_status(db, org.id, uuid.UUID(offer["id"]), "archived")
    assert [a["name"] for a in mm.list_matrix(db, org.id)["angles"]] == ["Buyers beat followers"]

    assert db.query(MessagingAngle).filter(MessagingAngle.org_id == other.id).count() == 0
    assert db.query(MessagingAngleSource).filter(MessagingAngleSource.org_id == org.id).count() == 5


def test_changed_transcript_is_retagged_and_rejections_are_cached(db, monkeypatch):
    from app.models.messaging_angle import MessagingAngleSource
    from app.services import messaging_matrix_service as mm

    fake = _fake(monkeypatch)
    org, _other, posts, _ads = _seed(db)
    posts[2].transcript = "Something with no clear angle in it at all."
    db.flush()

    mm.refresh_matrix(db, org.id)
    first = fake.calls["messaging_angle_tag"]
    rejected = (
        db.query(MessagingAngleSource)
        .filter(MessagingAngleSource.org_id == org.id, MessagingAngleSource.angle_id.is_(None))
        .count()
    )
    assert rejected == 1

    mm.refresh_matrix(db, org.id)
    assert fake.calls["messaging_angle_tag"] == first  # rejection cached by hash

    posts[2].transcript = FOLLOWERS
    db.flush()
    mm.refresh_matrix(db, org.id)
    assert fake.calls["messaging_angle_tag"] == first + 1
