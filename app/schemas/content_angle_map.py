from datetime import datetime
from typing import List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field


AngleCard = Literal["icp", "personal_brand"]
FormatStage = Literal["tof", "mof", "bof"]


class ContentAngleItemOut(BaseModel):
    id: str
    text: str
    manually_edited: bool = False


class ContentAngleMapOut(BaseModel):
    org_id: UUID
    organization_name: str
    icp_angles: List[ContentAngleItemOut] = Field(default_factory=list)
    personal_brand_angles: List[ContentAngleItemOut] = Field(default_factory=list)
    format_pills_tof: List[str] = Field(default_factory=list)
    format_pills_mof: List[str] = Field(default_factory=list)
    format_pills_bof: List[str] = Field(default_factory=list)
    icp_placeholders: List[str] = Field(default_factory=list)
    brand_placeholders: List[str] = Field(default_factory=list)
    last_generated_at: Optional[datetime] = None
    last_generated_icp_at: Optional[datetime] = None
    last_generated_brand_at: Optional[datetime] = None
    can_generate_icp: bool = False
    can_generate_brand: bool = False


class ContentAnglePatchBody(BaseModel):
    card: AngleCard
    id: str = Field(..., min_length=1, max_length=64)
    text: str = Field(..., min_length=1, max_length=160)


class ContentAngleRegenerateBody(BaseModel):
    card: AngleCard
    full: bool = False


class ContentAnglePillsPutBody(BaseModel):
    stage: FormatStage
    pills: List[str] = Field(default_factory=list, max_length=20)


class ContentAngleDeleteBody(BaseModel):
    card: AngleCard
    id: str = Field(..., min_length=1, max_length=64)
