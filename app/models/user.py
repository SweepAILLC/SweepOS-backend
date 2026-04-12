from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey, TypeDecorator
from sqlalchemy.dialects.postgresql import UUID, JSON, ENUM as PG_ENUM
import uuid
from datetime import datetime
import enum
from app.db.session import Base


class UserRole(str, enum.Enum):
    """Stored in PostgreSQL as native enum `userrole`: OWNER, ADMIN, MEMBER (uppercase labels)."""

    OWNER = "OWNER"
    ADMIN = "ADMIN"
    MEMBER = "MEMBER"


def parse_user_role_from_db(raw: object) -> UserRole:
    """Map DB `userrole` (any legacy casing) to UserRole."""
    if raw is None:
        return UserRole.ADMIN
    s = str(raw).strip()
    up = s.upper()
    if up == "OWNER":
        return UserRole.OWNER
    if up == "ADMIN":
        return UserRole.ADMIN
    if up == "MEMBER":
        return UserRole.MEMBER
    # legacy lowercase `member` from older migrations
    if s.lower() == "member":
        return UserRole.MEMBER
    return UserRole.ADMIN


def role_to_api(role: UserRole) -> str:
    """API / frontend expect lowercase owner | admin | member."""
    return role.value.lower()


def parse_user_role_from_api(raw: str) -> UserRole:
    """Parse settings/UI role strings into UserRole."""
    if not raw:
        return UserRole.MEMBER
    key = str(raw).strip().lower()
    if key == "owner":
        return UserRole.OWNER
    if key == "admin":
        return UserRole.ADMIN
    if key == "member":
        return UserRole.MEMBER
    return UserRole.MEMBER


_PG_USERROLE = PG_ENUM(
    "OWNER",
    "ADMIN",
    "MEMBER",
    name="userrole",
    create_type=False,
)


class PgUserRole(TypeDecorator):
    """
    Always bind PostgreSQL `userrole` labels as OWNER/ADMIN/MEMBER.

    Plain SQLEnum can still emit lowercase legacy strings if a User row was loaded with a bad
    Python value; that breaks sync when unrelated rows (e.g. Client) are flushed in the same session.
    """

    impl = String(32)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(_PG_USERROLE)
        return dialect.type_descriptor(String(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, UserRole):
            return value.value
        s = str(value).strip()
        up = s.upper()
        if up in ("OWNER", "ADMIN", "MEMBER"):
            return up
        low = s.lower()
        return {"owner": "OWNER", "admin": "ADMIN", "member": "MEMBER"}.get(low, "ADMIN")

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return parse_user_role_from_db(value)


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    email = Column(String, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    role = Column(PgUserRole(), default=UserRole.ADMIN, nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    fathom_api_key = Column(String, nullable=True)
    ai_profile = Column(JSON, nullable=True)

    __table_args__ = (
        {"schema": None},
    )
