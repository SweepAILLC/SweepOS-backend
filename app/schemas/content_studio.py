"""Pydantic models for Content Studio API."""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class SalesPlaybookOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source: Literal["fathom", "default"] = "default"
    paragraphs: List[str] = Field(default_factory=list)


class KnowledgePutBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    objections: List[str] = Field(default_factory=list)
    closing: List[str] = Field(default_factory=list)
    reframes: List[str] = Field(default_factory=list)


class KnowledgeOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    objections: List[str] = Field(default_factory=list)
    closing: List[str] = Field(default_factory=list)
    reframes: List[str] = Field(default_factory=list)


class StageConceptOut(BaseModel):
    """Single video concept (long-form or short-form). Bulleted concept only — never a script."""

    model_config = ConfigDict(extra="ignore")

    id: str
    format: Literal["long", "short"] = "short"
    title: str
    bullets: List[str] = Field(default_factory=list)
    why_for_icp: str = ""
    funnel_path_to_sale: str = ""


class StageOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Literal["TOF", "MOF", "BOF"]
    title: str
    intro: str = ""
    concepts: List[StageConceptOut] = Field(default_factory=list)


class ContentStudioBundleOut(BaseModel):
    """v3 bundle: TOF/MOF/BOF video concepts grounded purely in Fathom + ICP."""

    model_config = ConfigDict(extra="ignore")

    version: int = 3
    signals_fingerprint: str = ""
    batch_id: str = ""
    generated_at: Optional[str] = None
    source: Literal["llm", "default", "fathom"] = "llm"
    stages: List[StageOut] = Field(default_factory=list)


class CompletePatchBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    completed_idea_ids: List[str] = Field(default_factory=list, max_length=300)


class CompletePatchResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    completed_idea_ids: List[str] = Field(default_factory=list)
    batch_id: Optional[str] = None
    updated_at: Optional[str] = None


class TranscriptAnalyzeBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    transcript: str = Field(..., min_length=40, max_length=50000)
    purpose: Literal["TOF", "MOF", "BOF", "mixed"]
    mixed_note: Optional[str] = Field(default=None, max_length=2000)


class TranscriptAnalyzeResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    purpose: str
    analysis: Dict[str, Any]


class TranscriptListItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    purpose: str
    mixed_note: Optional[str] = None
    created_at: Optional[str] = None
    summary: Optional[str] = None


class TranscriptListResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    items: List[TranscriptListItem] = Field(default_factory=list)


class BootstrapResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    knowledge: KnowledgeOut
    sales_playbook: SalesPlaybookOut
    content_bundle: Optional[ContentStudioBundleOut] = None
    completed_idea_ids: List[str] = Field(default_factory=list)
    batch_id: Optional[str] = None


class ReanalyzeResponse(BaseModel):
    """POST /content-studio/reanalyze — Fathom pull + intelligence cache bust + bundle regen queued."""

    model_config = ConfigDict(extra="ignore")

    fathom_sync: Dict[str, Any] = Field(default_factory=dict)
    bundle_regenerating: bool = True
    health_clients_invalidated: int = 0
