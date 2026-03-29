#!/usr/bin/env python3
"""
Refresh all AI outputs for an organization (or all orgs).

What it refreshes:
  1. Health score caches — invalidated so next access recomputes (logic + AI overlay)
  2. Call insights — re-runs LLM analysis on Fathom call recordings with completed sentiment
  3. AI recommendation checklists — reset to lifecycle defaults so they pick up
     fresh call-insight merges and pipeline-priority ordering

Usage:
  # Refresh everything for a specific org
  python scripts/refresh_ai.py --org-id 00000000-0000-0000-0000-000000000001

  # Refresh everything for ALL orgs
  python scripts/refresh_ai.py --all

  # Refresh only health scores
  python scripts/refresh_ai.py --org-id <UUID> --health-only

  # Refresh only call insights
  python scripts/refresh_ai.py --org-id <UUID> --insights-only

  # Refresh only recommendation checklists
  python scripts/refresh_ai.py --org-id <UUID> --recommendations-only

  # Dry run (show what would be refreshed without making changes)
  python scripts/refresh_ai.py --org-id <UUID> --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.core.config import settings  # noqa: E402
from app.db.session import SessionLocal  # noqa: E402
from app.models.client import Client  # noqa: E402
from app.models.client_ai_recommendation_state import ClientAIRecommendationState  # noqa: E402
from app.models.client_health_score_cache import ClientHealthScoreCache  # noqa: E402
from app.models.fathom_call_record import FathomCallRecord  # noqa: E402
from app.models.client_call_insight import ClientCallInsight  # noqa: E402
from app.models.organization import Organization  # noqa: E402


def resolve_org_ids(db, org_id_str: Optional[str], all_orgs: bool) -> List[uuid.UUID]:
    if all_orgs:
        rows = db.query(Organization.id).all()
        return [r[0] for r in rows]
    if org_id_str:
        try:
            return [uuid.UUID(org_id_str)]
        except ValueError:
            print(f"Error: invalid UUID '{org_id_str}'")
            sys.exit(1)
    print("Error: provide --org-id <UUID> or --all")
    sys.exit(1)


def refresh_health_scores(db, org_id: uuid.UUID, dry_run: bool) -> int:
    rows = (
        db.query(ClientHealthScoreCache)
        .filter(ClientHealthScoreCache.org_id == org_id)
        .all()
    )
    count = len(rows)
    if dry_run:
        print(f"  [dry-run] Would invalidate {count} health score cache(s)")
        return count
    for row in rows:
        db.delete(row)
    if count:
        db.commit()
    print(f"  Invalidated {count} health score cache(s)")
    return count


def refresh_call_insights(db, org_id: uuid.UUID, dry_run: bool) -> int:
    from app.services.call_insight_service import run_call_insight_for_fathom_record

    records = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.sentiment_status == "complete",
            FathomCallRecord.client_id.isnot(None),
        )
        .all()
    )

    if dry_run:
        print(f"  [dry-run] Would re-analyze {len(records)} Fathom recording(s)")
        return len(records)

    ok = skip = fail = 0
    for rec in records:
        db.query(ClientCallInsight).filter(
            ClientCallInsight.fathom_call_record_id == rec.id
        ).delete()
        db.commit()

        status, detail = run_call_insight_for_fathom_record(
            db, org_id, rec.id, bypass_cooldown=True
        )
        if status == "ok":
            ok += 1
        elif status == "skipped":
            skip += 1
        else:
            fail += 1
        print(f"    record {rec.id}: {status}" + (f" ({detail})" if detail else ""))

    print(f"  Call insights: {ok} ok, {skip} skipped, {fail} failed (of {len(records)} total)")
    return ok


def refresh_recommendations(db, org_id: uuid.UUID, dry_run: bool) -> int:
    from app.services.client_ai_recommendations_service import (
        default_actions_for_client,
    )

    rows = (
        db.query(ClientAIRecommendationState)
        .filter(ClientAIRecommendationState.org_id == org_id)
        .all()
    )

    if dry_run:
        print(f"  [dry-run] Would reset {len(rows)} recommendation checklist(s)")
        return len(rows)

    refreshed = 0
    for row in rows:
        client = db.query(Client).filter(Client.id == row.client_id).first()
        if not client:
            continue

        existing_actions = row.actions if isinstance(row.actions, list) else []
        completed_ids = {
            str(a.get("id"))
            for a in existing_actions
            if isinstance(a, dict) and a.get("completed")
        }
        call_insight_actions = [
            a for a in existing_actions
            if isinstance(a, dict) and str(a.get("source", "")) == "call_insight"
        ]

        headline, new_defaults = default_actions_for_client(client)
        merged = []
        for a in new_defaults:
            ac = dict(a)
            if str(ac.get("id")) in completed_ids:
                ac["completed"] = True
                ac["completed_at"] = datetime.now(timezone.utc).isoformat()
            merged.append(ac)

        for cia in call_insight_actions:
            merged.append(dict(cia))

        row.headline = headline
        row.actions = merged
        row.updated_at = datetime.now(timezone.utc)
        refreshed += 1

    if refreshed:
        db.commit()
    print(f"  Reset {refreshed} recommendation checklist(s) (preserved completions + call-insight items)")
    return refreshed


def main():
    parser = argparse.ArgumentParser(description="Refresh AI outputs for SweepOS")
    parser.add_argument("--org-id", type=str, default=None, help="Organization UUID")
    parser.add_argument("--all", action="store_true", help="Refresh all organizations")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be refreshed without changes")
    parser.add_argument("--health-only", action="store_true", help="Only refresh health scores")
    parser.add_argument("--insights-only", action="store_true", help="Only refresh call insights")
    parser.add_argument("--recommendations-only", action="store_true", help="Only refresh recommendation checklists")
    args = parser.parse_args()

    do_all = not args.health_only and not args.insights_only and not args.recommendations_only

    db = SessionLocal()
    try:
        org_ids = resolve_org_ids(db, args.org_id, args.all)
        print(f"\nRefreshing AI outputs for {len(org_ids)} org(s){'  [DRY RUN]' if args.dry_run else ''}\n")

        total_health = total_insights = total_recs = 0

        for oid in org_ids:
            client_count = db.query(Client).filter(Client.org_id == oid).count()
            print(f"Org {oid}  ({client_count} client(s))")

            if do_all or args.health_only:
                total_health += refresh_health_scores(db, oid, args.dry_run)

            if do_all or args.insights_only:
                total_insights += refresh_call_insights(db, oid, args.dry_run)

            if do_all or args.recommendations_only:
                total_recs += refresh_recommendations(db, oid, args.dry_run)

            print()

        print("Done.")
        if do_all or args.health_only:
            print(f"  Health caches invalidated: {total_health}")
        if do_all or args.insights_only:
            print(f"  Call insights refreshed:   {total_insights}")
        if do_all or args.recommendations_only:
            print(f"  Recommendations reset:     {total_recs}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
