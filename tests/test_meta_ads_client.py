"""Meta Ads direct Graph API client: OAuth state helpers (no network/DB
needed)."""
import uuid

import pytest

from app.services.meta_ads_client import MetaAdsConfigError, sign_state, verify_state


def test_sign_state_round_trips_org_id():
    org_id = uuid.uuid4()
    state = sign_state(org_id)
    assert verify_state(state) == org_id


def test_verify_state_rejects_tampered_signature():
    org_id = uuid.uuid4()
    state = sign_state(org_id)
    payload_b64, _sig = state.split(".", 1)
    tampered = payload_b64 + ".0" * 16
    with pytest.raises(MetaAdsConfigError):
        verify_state(tampered)
