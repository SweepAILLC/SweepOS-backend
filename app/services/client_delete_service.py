"""Delete a client and all org-scoped rows that reference clients.id."""
from __future__ import annotations

import uuid
from typing import Union
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.client_checkin import ClientCheckIn
from app.models.event import Event
from app.models.funnel import Funnel
from app.models.manual_payment import ManualPayment
from app.models.recommendation import Recommendation
from app.models.stripe_payment import StripePayment
from app.models.stripe_subscription import StripeSubscription
from app.models.stripe_treasury_transaction import StripeTreasuryTransaction
from app.models.whop_payment import WhopPayment

try:
    from app.models.client_call_insight import ClientCallInsight, ClientInsightSummary
    from app.models.client_health_score_cache import ClientHealthScoreCache
    from app.models.client_ai_recommendation_state import ClientAIRecommendationState
    from app.models.health_outcome_snapshot import HealthOutcomeSnapshot
except ImportError:
    ClientCallInsight = ClientInsightSummary = None  # type: ignore[misc, assignment]
    ClientHealthScoreCache = ClientAIRecommendationState = HealthOutcomeSnapshot = None  # type: ignore[misc, assignment]

try:
    from app.models.fathom_call_record import FathomCallRecord
except ImportError:
    FathomCallRecord = None  # type: ignore[misc, assignment]

try:
    from app.models.automation import AutomationEmailJob
except ImportError:
    AutomationEmailJob = None  # type: ignore[misc, assignment]


def _as_uuid(client_id: Union[str, UUID]) -> UUID:
    return client_id if isinstance(client_id, uuid.UUID) else UUID(str(client_id))


def purge_client_dependencies(db: Session, org_id: UUID, client_id: Union[str, UUID]) -> None:
    """
    Remove or detach all foreign-key dependents before deleting the Client row.

    Manual payments and check-ins use NOT NULL client_id and must be deleted.
    Fathom and other nullable FKs are set to NULL so client delete can succeed.
    """
    cid = _as_uuid(client_id)

    # 1:1 / insight rows use client_id as PK — must DELETE (ORM cannot SET NULL on delete).
    if ClientCallInsight is not None:
        db.query(ClientCallInsight).filter(
            ClientCallInsight.client_id == cid,
            ClientCallInsight.org_id == org_id,
        ).delete(synchronize_session=False)

    if ClientInsightSummary is not None:
        db.query(ClientInsightSummary).filter(
            ClientInsightSummary.client_id == cid,
            ClientInsightSummary.org_id == org_id,
        ).delete(synchronize_session=False)

    if ClientHealthScoreCache is not None:
        db.query(ClientHealthScoreCache).filter(
            ClientHealthScoreCache.client_id == cid,
            ClientHealthScoreCache.org_id == org_id,
        ).delete(synchronize_session=False)

    if ClientAIRecommendationState is not None:
        db.query(ClientAIRecommendationState).filter(
            ClientAIRecommendationState.client_id == cid,
            ClientAIRecommendationState.org_id == org_id,
        ).delete(synchronize_session=False)

    if HealthOutcomeSnapshot is not None:
        db.query(HealthOutcomeSnapshot).filter(
            HealthOutcomeSnapshot.client_id == cid,
            HealthOutcomeSnapshot.org_id == org_id,
        ).delete(synchronize_session=False)

    db.query(ManualPayment).filter(
        ManualPayment.client_id == cid,
        ManualPayment.org_id == org_id,
    ).delete(synchronize_session=False)

    db.query(ClientCheckIn).filter(
        ClientCheckIn.client_id == cid,
        ClientCheckIn.org_id == org_id,
    ).delete(synchronize_session=False)

    db.query(StripePayment).filter(
        StripePayment.client_id == cid,
        StripePayment.org_id == org_id,
    ).update({StripePayment.client_id: None}, synchronize_session=False)

    db.query(StripeSubscription).filter(
        StripeSubscription.client_id == cid,
        StripeSubscription.org_id == org_id,
    ).update({StripeSubscription.client_id: None}, synchronize_session=False)

    db.query(StripeTreasuryTransaction).filter(
        StripeTreasuryTransaction.client_id == cid,
        StripeTreasuryTransaction.org_id == org_id,
    ).update({StripeTreasuryTransaction.client_id: None}, synchronize_session=False)

    db.query(WhopPayment).filter(
        WhopPayment.client_id == cid,
        WhopPayment.org_id == org_id,
    ).update({WhopPayment.client_id: None}, synchronize_session=False)

    db.query(Event).filter(
        Event.client_id == cid,
        Event.org_id == org_id,
    ).update({Event.client_id: None}, synchronize_session=False)

    db.query(Funnel).filter(
        Funnel.client_id == cid,
        Funnel.org_id == org_id,
    ).update({Funnel.client_id: None}, synchronize_session=False)

    db.query(Recommendation).filter(
        Recommendation.client_id == cid,
        Recommendation.org_id == org_id,
    ).update({Recommendation.client_id: None}, synchronize_session=False)

    if FathomCallRecord is not None:
        db.query(FathomCallRecord).filter(
            FathomCallRecord.client_id == cid,
            FathomCallRecord.org_id == org_id,
        ).update({FathomCallRecord.client_id: None}, synchronize_session=False)

    if AutomationEmailJob is not None:
        db.query(AutomationEmailJob).filter(
            AutomationEmailJob.client_id == cid,
            AutomationEmailJob.org_id == org_id,
        ).delete(synchronize_session=False)
