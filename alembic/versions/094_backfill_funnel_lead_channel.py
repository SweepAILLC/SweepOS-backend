"""Backfill source_channel / source_funnel_id for funnel leads captured before 091.

091 added the attribution columns with no backfill, so leads captured through a
tracked funnel before it shipped show as Organic. A client counts as a funnel
lead when it is linked to a funnel in the same org, either via
meta.prospect.funnel_id (stamped at capture) or via funnel_lead_notifications.
Only rows with no channel on record are touched (first-touch attribution: a
known channel is never overwritten). Remaining NULL rows become 'organic',
matching the column default for every non-funnel creation path.

Revision ID: 094
Revises: 093
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "094"
down_revision = "093"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1) Prospect-meta stamp (canonical). Regex guard skips malformed ids instead
    #    of aborting the whole statement on a bad ::uuid cast.
    op.execute(
        """
        UPDATE clients c
        SET source_channel = 'paid',
            source_funnel_id = f.id
        FROM funnels f
        WHERE c.source_channel IS NULL
          AND c.meta IS NOT NULL
          AND (c.meta -> 'prospect' ->> 'funnel_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND f.id = (c.meta -> 'prospect' ->> 'funnel_id')::uuid
          AND f.org_id = c.org_id
        """
    )
    # 2) Historical digest-queue links (earliest capture wins). The table is
    #    created lazily by app code, so it may not exist on every database.
    has_notifications = op.get_bind().execute(
        sa.text("SELECT to_regclass('public.funnel_lead_notifications') IS NOT NULL")
    ).scalar()
    if has_notifications:
        _backfill_from_notifications()
    # 3) Everyone else with no channel on record came in organically.
    op.execute("UPDATE clients SET source_channel = 'organic' WHERE source_channel IS NULL")


def _backfill_from_notifications() -> None:
    op.execute(
        """
        UPDATE clients c
        SET source_channel = 'paid',
            source_funnel_id = n.funnel_id
        FROM (
            SELECT DISTINCT ON (client_id) client_id, funnel_id, org_id
            FROM funnel_lead_notifications
            WHERE client_id IS NOT NULL AND funnel_id IS NOT NULL
            ORDER BY client_id, created_at ASC
        ) n
        JOIN funnels f ON f.id = n.funnel_id
        WHERE c.source_channel IS NULL
          AND c.id = n.client_id
          AND n.org_id = c.org_id
          AND f.org_id = c.org_id
        """
    )


def downgrade() -> None:
    # Data-only backfill; the pre-backfill NULLs are not recoverable and the
    # values written are correct, so downgrade is a no-op.
    pass
