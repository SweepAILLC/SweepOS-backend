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


class SectionIdeaOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    stage: Literal["TOF", "MOF", "BOF"]
    hook: str
    concept: str
    why_it_works: str
    format: str = "reel"


class ContentSectionOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    title: str
    body: str
    ideas: List[SectionIdeaOut] = Field(default_factory=list)


class VoiceMarketingOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    title: str
    body: str
    bullets: List[str] = Field(default_factory=list)


class ContentStudioBundleOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: int = 2
    signals_fingerprint: str = ""
    batch_id: str = ""
    generated_at: Optional[str] = None
    source: Literal["llm", "default", "fathom"] = "llm"
    sections: List[ContentSectionOut] = Field(default_factory=list)
    voice_marketing: VoiceMarketingOut = Field(default_factory=lambda: VoiceMarketingOut(title="", body=""))


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
