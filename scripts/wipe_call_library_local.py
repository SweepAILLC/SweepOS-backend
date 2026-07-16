#!/usr/bin/env python3
"""
Wipe Call Library + Fathom call rows from the *local* database only.

Safety: refuses to run unless ENVIRONMENT is development/dev/local and the
DATABASE_URL host looks local (localhost, 127.0.0.1, or docker service `db`).

Usage (from repo root):
  make wipe-call-library
  # or
  docker-compose -f docker/docker-compose.yml exec backend python scripts/wipe_call_library_local.py
"""
from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

# Match other backend/scripts entrypoints (python scripts/... puts scripts/ on sys.path[0]).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _is_local_dev() -> bool:
    env = (os.environ.get("ENVIRONMENT") or "").strip().lower()
    return env in {"development", "dev", "local"}


def _db_host_is_local(database_url: str) -> bool:
    host = (urlparse(database_url).hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "db", "::1"} or host.endswith(".local")


def main() -> int:
    # Prefer app settings when available (Docker mounts backend + .env).
    try:
        from app.core.config import settings

        database_url = (settings.DATABASE_URL or "").strip()
    except Exception:
        database_url = (os.environ.get("DATABASE_URL") or "").strip()

    if not _is_local_dev():
        print(
            "Refusing: ENVIRONMENT must be development/dev/local "
            f"(got {os.environ.get('ENVIRONMENT')!r}).",
            file=sys.stderr,
        )
        return 1
    if not database_url or not _db_host_is_local(database_url):
        print(
            "Refusing: DATABASE_URL does not look like local Docker/Postgres "
            f"(host must be localhost/127.0.0.1/db). url={database_url!r}",
            file=sys.stderr,
        )
        return 1

    from sqlalchemy import text
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        before = db.execute(
            text(
                """
                SELECT
                  (SELECT count(*) FROM call_library_reports) AS reports,
                  (SELECT count(*) FROM client_call_insights) AS insights,
                  (SELECT count(*) FROM fathom_call_records) AS fathom
                """
            )
        ).mappings().one()
        print(
            f"Before: call_library_reports={before['reports']} "
            f"client_call_insights={before['insights']} "
            f"fathom_call_records={before['fathom']}"
        )

        # Reports/insights FK → fathom; delete children first then fathom rows.
        deleted_reports = db.execute(text("DELETE FROM call_library_reports")).rowcount
        deleted_insights = db.execute(text("DELETE FROM client_call_insights")).rowcount
        deleted_fathom = db.execute(text("DELETE FROM fathom_call_records")).rowcount
        db.commit()

        after = db.execute(
            text(
                """
                SELECT
                  (SELECT count(*) FROM call_library_reports) AS reports,
                  (SELECT count(*) FROM client_call_insights) AS insights,
                  (SELECT count(*) FROM fathom_call_records) AS fathom
                """
            )
        ).mappings().one()
        print(
            f"Deleted: reports={deleted_reports} insights={deleted_insights} "
            f"fathom={deleted_fathom}"
        )
        print(
            f"After: call_library_reports={after['reports']} "
            f"client_call_insights={after['insights']} "
            f"fathom_call_records={after['fathom']}"
        )
        print(
            "Done. Org Fathom webhook URL/secret unchanged. "
            "Pull new meetings via Integrations sync or inbound webhook."
        )
        return 0
    except Exception as exc:
        db.rollback()
        print(f"Wipe failed: {exc}", file=sys.stderr)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
