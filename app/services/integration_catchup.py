"""Worker safety-net pulls for calendar (Cal.com / Calendly) and Whop.

Booking + payment side effects (Discord new_booking / new_transaction, post-booking
automations, pipeline lifecycle) fire from webhooks and from pull-sync. Before this,
the calendar and Whop pulls only ran when someone had the app open (the browser's
check-in sync loop / manual Whop sync), so a missed or unregistered webhook meant no
notification until a user loaded the dashboard. The worker now runs the same pulls on
an interval. Side effects stay exactly-once via integration_side_effects.claim_dispatch.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# Skip a Whop org whose last sync/webhook is fresher than this.
WHOP_FRESH_WINDOW = timedelta(minutes=4)


def _audit_user_id(db, org_id: uuid.UUID) -> Optional[uuid.UUID]:
    """Any member of the org — sync_all_checkins only uses user_id for token-decrypt audit rows."""
    from app.models.user_organization import UserOrganization

    row = (
        db.query(UserOrganization.user_id)
        .filter(UserOrganization.org_id == org_id)
        .order_by(UserOrganization.is_primary.desc(), UserOrganization.created_at.asc())
        .first()
    )
    return row[0] if row else None


def catchup_calendar_for_all_orgs() -> dict[str, int]:
    """Run the same check-in sync the dashboard runs, for every calendar-connected org."""
    from app.models.oauth_token import OAuthProvider, OAuthToken
    from app.services.checkin_sync import sync_all_checkins
    from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

    db = SessionLocal()
    try:
        org_ids = sorted(
            {
                org_id
                for (org_id,) in db.query(OAuthToken.org_id)
                .filter(OAuthToken.provider.in_((OAuthProvider.CALCOM, OAuthProvider.CALENDLY)))
                .all()
            },
            key=str,
        )
    finally:
        db.close()

    synced = failed = skipped = new_bookings = 0
    for org_id in org_ids:
        bg = SessionLocal()
        try:
            user_id = _audit_user_id(bg, org_id)
            if user_id is None:
                skipped += 1
                continue
            result = sync_all_checkins(bg, org_id, user_id)
            invalidate_terminal_monthly_trends_cache(org_id)
            new_bookings += int(result.get("new_bookings_calcom") or 0) + int(
                result.get("new_bookings_calendly") or 0
            )
            synced += 1
        except Exception:
            failed += 1
            logger.exception("integration_catchup: calendar sync failed org=%s", org_id)
            try:
                bg.rollback()
            except Exception:
                pass
        finally:
            bg.close()
    return {"orgs": len(org_ids), "synced": synced, "failed": failed, "skipped": skipped, "new_bookings": new_bookings}


def catchup_whop_for_all_orgs() -> dict[str, int]:
    """Incremental Whop payment pull for every Whop-connected org."""
    from app.models.oauth_token import OAuthProvider, OAuthToken
    from app.services.whop_sync import sync_whop_incremental

    db = SessionLocal()
    try:
        org_ids = [
            org_id
            for (org_id,) in db.query(OAuthToken.org_id)
            .filter(OAuthToken.provider == OAuthProvider.WHOP, OAuthToken.access_token.isnot(None))
            .all()
        ]
    finally:
        db.close()

    synced = failed = skipped = 0
    for org_id in org_ids:
        bg = SessionLocal()
        try:
            token = (
                bg.query(OAuthToken)
                .filter(OAuthToken.provider == OAuthProvider.WHOP, OAuthToken.org_id == org_id)
                .first()
            )
            if token is None:
                skipped += 1
                continue
            last = max((t for t in (token.last_sync_at, token.last_webhook_processed_at) if t), default=None)
            if last and datetime.utcnow() - last < WHOP_FRESH_WINDOW:
                skipped += 1
                continue
            result = sync_whop_incremental(bg, org_id=org_id, force_full=False)
            if result.get("error"):
                failed += 1
                logger.warning("integration_catchup: whop sync failed org=%s error=%s", org_id, result.get("error"))
            else:
                synced += 1
        except Exception:
            failed += 1
            logger.exception("integration_catchup: whop sync failed org=%s", org_id)
            try:
                bg.rollback()
            except Exception:
                pass
        finally:
            bg.close()
    return {"orgs": len(org_ids), "synced": synced, "failed": failed, "skipped": skipped}
