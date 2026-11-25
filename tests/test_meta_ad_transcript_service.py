"""Meta ad transcription: source preference (Instagram -> Whisper -> ad copy), no network."""
import uuid
from types import SimpleNamespace

import pytest

from app.services import meta_ad_transcript_service as svc


class _FakeDB:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


ORG = uuid.uuid4()


def _ad(**kw):
    base = dict(
        id=uuid.uuid4(),
        org_id=ORG,
        meta_ad_id="a1",
        video_id=None,
        instagram_media_id=None,
        ad_copy=None,
        transcript=None,
        transcript_source=None,
        transcript_status="unavailable",
        transcript_error=None,
        transcribed_at=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    def boom(**_):
        raise AssertionError("unexpected Meta call")

    def no_download(url):
        raise AssertionError("unexpected download")

    monkeypatch.setattr(svc.mac, "get_ad_creative", boom)
    monkeypatch.setattr(svc.mac, "get_video_source_url", boom)
    monkeypatch.setattr(svc.its, "_download_media", no_download)
    monkeypatch.setattr(svc, "_find_instagram_media", lambda db, org, ig: None)


def test_reuses_existing_instagram_transcript_without_whisper(monkeypatch):
    media = SimpleNamespace(id=uuid.uuid4(), transcript="reel words", transcript_status="ok", media_url="x")
    monkeypatch.setattr(svc, "_find_instagram_media", lambda db, org, ig: media)
    ad = _ad(instagram_media_id="1789", video_id="v1")
    svc.transcribe_ad(_FakeDB(), ORG, ad, "tok")
    assert ad.transcript == "reel words"
    assert ad.transcript_source == "instagram"
    assert ad.transcript_status == "ok"


def test_whisper_on_video_source(monkeypatch):
    monkeypatch.setattr(svc.mac, "get_video_source_url", lambda **kw: "https://signed/video.mp4")
    monkeypatch.setattr(svc.its, "_download_media", lambda url: b"mp4")
    seen = {}

    def fake_whisper(audio, filename, org_id, *, feature):
        seen["feature"] = feature
        seen["org"] = org_id
        return "ad words"

    monkeypatch.setattr(svc.its, "_whisper_transcribe", fake_whisper)
    ad = _ad(video_id="v1", ad_copy="copy")
    svc.transcribe_ad(_FakeDB(), ORG, ad, "tok")
    assert ad.transcript == "ad words"
    assert ad.transcript_source == "whisper"
    assert seen == {"feature": "meta_ad_transcript", "org": ORG}


def test_falls_back_to_ad_copy_when_video_unreadable(monkeypatch):
    monkeypatch.setattr(svc.mac, "get_video_source_url", lambda **kw: None)
    ad = _ad(video_id="v1", ad_copy="Headline\n\nBody")
    svc.transcribe_ad(_FakeDB(), ORG, ad, "tok")
    assert ad.transcript == "Headline\n\nBody"
    assert ad.transcript_source == "ad_copy"
    assert ad.transcript_status == "ok"


def test_whisper_too_large_falls_back_to_copy_and_keeps_error(monkeypatch):
    monkeypatch.setattr(svc.mac, "get_video_source_url", lambda **kw: "https://signed/v.mp4")

    def too_big(url):
        raise svc.its.TranscriptUnavailableError("over 25MB")

    monkeypatch.setattr(svc.its, "_download_media", too_big)
    ad = _ad(video_id="v1", ad_copy="copy")
    svc.transcribe_ad(_FakeDB(), ORG, ad, "tok")
    assert ad.transcript_source == "ad_copy"
    assert "25MB" in ad.transcript_error


def test_failed_with_no_copy_marks_failed(monkeypatch):
    def api_down(**kw):
        raise RuntimeError("meta 500")

    monkeypatch.setattr(svc.mac, "get_video_source_url", api_down)
    ad = _ad(video_id="v1")
    svc.transcribe_ad(_FakeDB(), ORG, ad, "tok")
    assert ad.transcript_status == "failed"
    assert ad.transcript is None


def test_fetches_creative_once_for_ads_synced_before_widening(monkeypatch):
    monkeypatch.setattr(svc.mac, "get_ad_creative", lambda **kw: {"title": "T", "body": "B"})
    ad = _ad(creative_id=None, creative_thumbnail_url=None)
    svc.transcribe_ad(_FakeDB(), ORG, ad, "tok")
    assert ad.ad_copy == "T\n\nB"
    assert ad.transcript_source == "ad_copy"


def test_already_ok_is_untouched():
    ad = _ad(transcript="done", transcript_status="ok")
    db = _FakeDB()
    svc.transcribe_ad(db, ORG, ad, "tok")
    assert db.commits == 0


def test_rejects_other_orgs_ad():
    with pytest.raises(ValueError):
        svc.transcribe_ad(_FakeDB(), ORG, _ad(org_id=uuid.uuid4()), "tok")


def test_spend_floor_is_fifty_dollars():
    assert svc.AD_SPEND_FLOOR_CENTS == 5000
