from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date
from uuid import UUID


class PortalTodoCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=500)
    description: Optional[str] = None
    due_date: Optional[date] = None


class PortalTodoUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=500)
    description: Optional[str] = None
    completed: Optional[bool] = None
    due_date: Optional[date] = None


class PortalTodoResponse(BaseModel):
    id: UUID
    org_id: UUID
    title: str
    description: Optional[str] = None
    completed: bool
    due_date: Optional[date] = None
    created_by: Optional[UUID] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PortalSharedPadUpdate(BaseModel):
    content: str = Field(default="", max_length=200_000)
    """Full notepad body (last-write-wins with revision bump)."""
    base_revision: Optional[int] = None
    """Optional: client’s last seen revision (informational; server always accepts writes)."""


class PortalSharedPadCreate(BaseModel):
    title: Optional[str] = Field(None, max_length=120)


class PortalSharedPadRename(BaseModel):
    title: str = Field(..., min_length=1, max_length=120)


class PortalSharedPadSummary(BaseModel):
    id: UUID
    org_id: UUID
    title: str
    sort_order: int = 0
    revision: int = 1
    updated_by_name: Optional[str] = None
    updated_at: datetime

    class Config:
        from_attributes = True


class PortalSharedPadResponse(BaseModel):
    id: UUID
    org_id: UUID
    title: str = "Shared space"
    sort_order: int = 0
    content: str
    revision: int
    updated_by: Optional[UUID] = None
    updated_by_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    unchanged: bool = False
    """True when GET used since_revision and nothing changed (content may be empty)."""

    class Config:
        from_attributes = True


class PortalSharedPadDefaultUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=120)
    content: Optional[str] = Field(None, max_length=200_000)


class PortalSharedPadDefaultResponse(BaseModel):
    title: str
    content: str
    updated_by_name: Optional[str] = None
    updated_at: Optional[datetime] = None


class FunnelSimulatorScenarioCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    mode: str = Field(..., pattern="^(paid_vsl|organic_dm)$")
    funnel_id: Optional[UUID] = None
    lookback_days: str = Field(default="90", max_length=16)
    inputs: dict = Field(default_factory=dict)


class FunnelSimulatorScenarioUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=120)
    mode: Optional[str] = Field(None, pattern="^(paid_vsl|organic_dm)$")
    funnel_id: Optional[UUID] = None
    lookback_days: Optional[str] = Field(None, max_length=16)
    inputs: Optional[dict] = None


class FunnelSimulatorScenarioResponse(BaseModel):
    id: UUID
    org_id: UUID
    name: str
    mode: str
    funnel_id: Optional[UUID] = None
    lookback_days: str
    inputs: dict = Field(default_factory=dict)
    created_by: Optional[UUID] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
