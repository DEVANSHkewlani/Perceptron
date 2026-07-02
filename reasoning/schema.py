from __future__ import annotations
import uuid
from typing import Any
from pydantic import BaseModel, Field, field_validator, model_validator


class RootCauseHypothesis(BaseModel):
    hypothesis: str   = Field(description="Plain language causal explanation")
    confidence: float = Field(ge=0.0, le=1.0)
    evidence:   list[str] = Field(default_factory=list,
                                  description="Supporting evidence from situation summary")


class AlternativeAction(BaseModel):
    action:     str
    parameters: dict[str, Any] = Field(default_factory=dict)
    rationale:  str


class DecisionObject(BaseModel):
    decision_id:            str = Field(
        default_factory=lambda: f"dec_{uuid.uuid4().hex[:10]}"
    )
    situation_assessment:   str   = Field(
        description="Plain language summary of what the reasoning engine understands"
    )
    root_cause_hypothesis:  RootCauseHypothesis
    recommended_action:     str   = Field(
        description="Exact key from the actions vocabulary"
    )
    action_parameters:      dict[str, Any] = Field(default_factory=dict)
    confidence:             float = Field(ge=0.0, le=1.0)
    requires_human_approval: bool
    alternative_actions:    list[AlternativeAction] = Field(
        default_factory=list, max_length=2
    )
    reasoning_trace:        str   = Field(
        description="Step-by-step explanation for observability"
    )

    @field_validator("recommended_action")
    @classmethod
    def action_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("recommended_action cannot be empty")
        return v.strip()

    @model_validator(mode="after")
    def auto_require_approval_on_low_confidence(self) -> DecisionObject:
        if self.confidence < 0.65:
            self.requires_human_approval = True
        return self

    def to_episodic_record(self, situation_summary: dict) -> dict:
        """Serialize for storage in episodic memory — used as future experience."""
        return {
            "event_type":        "reasoning_completed",
            "situation_summary": situation_summary,
            "decision":          self.model_dump(),
            "outcome":           None,    # filled by feedback loop (Phase 10)
        }
