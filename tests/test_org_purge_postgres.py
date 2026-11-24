"""Org delete purge against a real Postgres (it reads pg_catalog, so SQLite can't run it).

Runs when TEST_POSTGRES_URL points at a migrated database, e.g. inside the dev container:
    TEST_POSTGRES_URL=$DATABASE_URL pytest tests/test_org_purge_postgres.py
Everything happens in one transaction that is rolled back.
"""
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

PG_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="TEST_POSTGRES_URL not set")


@pytest.fixture()
def db():
    engine = create_engine(PG_URL)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
        engine.dispose()


def _org_tables(db):
    return [
        r[0]
        for r in db.execute(
            text(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'org_id' AND NOT a.attisdropped "
                "WHERE n.nspname = 'public' AND c.relkind = 'r'"
            )
        )
    ]


def _counts(db, tables, org_id):
    return {t: db.execute(text(f'SELECT count(*) FROM "{t}" WHERE org_id = :o'), {"o": org_id}).scalar() for t in tables}


def test_purge_removes_org_and_never_touches_another_org(db):
    from app.api.admin import _purge_organization_rows
    from app.models.client import Client
    from app.models.client_checkin import ClientCheckIn
    from app.models.funnel import Funnel
    from app.models.organization import Organization
    from app.models.user import User

    other = Organization(id=uuid.uuid4(), name="ZZ other org")
    x, ux, cx, fx, cy, other_ci = (uuid.uuid4() for _ in range(6))
    db.add_all([other, Organization(id=x, name="ZZ purge org")])
    db.flush()
    db.add(User(id=ux, org_id=x, email=f"zz-{uuid.uuid4().hex[:8]}@example.com"))
    db.flush()
    db.add_all([Client(id=cx, org_id=x, email="cx@example.com"), Funnel(id=fx, org_id=x, name="ZZ funnel")])
    db.add(Client(id=cy, org_id=other.id, email="cy@example.com"))
    db.flush()
    db.execute(text("UPDATE clients SET source_funnel_id = :f WHERE id = :c"), {"f": fx, "c": cx})
    start = datetime.now(timezone.utc)
    db.add(
        ClientCheckIn(
            org_id=x, client_id=cx, event_id=uuid.uuid4().hex, provider="calcom",
            start_time=start, attendee_email="cx@example.com", host_user_id=ux,
        )
    )
    # Another org's call hosted by the deleted org's user.
    db.add(
        ClientCheckIn(
            id=other_ci, org_id=other.id, client_id=cy, event_id=uuid.uuid4().hex, provider="calcom",
            start_time=start, attendee_email="cy@example.com", host_user_id=ux,
        )
    )
    db.flush()

    tables = _org_tables(db)
    other_before = _counts(db, tables, other.id)
    assert sum(_counts(db, tables, x).values()) >= 4

    with db.begin_nested():
        _purge_organization_rows(db, x)
    db.expunge_all()

    assert sum(_counts(db, tables, x).values()) == 0
    assert db.execute(text("SELECT count(*) FROM organizations WHERE id = :o"), {"o": x}).scalar() == 0
    assert db.execute(text("SELECT count(*) FROM users WHERE id = :u"), {"u": ux}).scalar() == 0
    row = db.execute(text("SELECT host_user_id FROM client_check_ins WHERE id = :i"), {"i": other_ci}).first()
    assert row is not None and row[0] is None  # kept, unlinked
    assert _counts(db, tables, other.id) == other_before
