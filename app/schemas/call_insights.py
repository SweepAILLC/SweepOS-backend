"""API schemas for call insights (ROI, clips, board tags)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ClientInsightSummaryOut(BaseModel):
    headline: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    last_call_at: Optional[str] = None
    last_insight_at: Optional[str] = None


class CallInsightPerCallOut(BaseModel):
    id: str
    fathom_call_record_id: str
    fathom_recording_id: Optional[int] = None
    meeting_at: Optional[str] = None
    status: str
    computed_at: Optional[str] = None
    insight: Optional[Dict[str, Any]] = None  # complete rows; null if failed/skipped
    failure_reason: Optional[str] = None


class CallInsightsRollupOut(BaseModel):
    """Aggregated call context for the intelligence UI (not per-call sections)."""

    client_state_synthesis: str = ""
    accumulated_priorities: List[str] = Field(default_factory=list)
    accumulated_call_suggestions: List[Dict[str, Any]] = Field(default_factory=list)
    accumulated_clips: List[Dict[str, Any]] = Field(default_factory=list)
    accumulated_wins: List[str] = Field(default_factory=list)
    accumulated_testimonial_stories: List[str] = Field(default_factory=list)
    prospect_voice_profile: Dict[str, Any] = Field(default_factory=dict)


class ClientCallInsightsResponse(BaseModel):
    client_id: str
    summary: Optional[ClientInsightSummaryOut] = None
    insights: List[CallInsightPerCallOut] = Field(default_factory=list)
    rollup: Optional[CallInsightsRollupOut] = None


class CallInsightTagEntry(BaseModel):
    tags: List[str] = Field(default_factory=list)
    headline: str = ""


class RefreshCallInsightsResponse(BaseModel):
    status: str
    detail: Optional[Dict[str, Any]] = None
