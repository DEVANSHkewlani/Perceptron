"""
Plan schema — the complete plan object passed to the Execution Layer.
PlanStep: one concrete action with timeout, dependency, and failure behaviour.
Plan: the full executable strategy produced by PlanGenerator.
"""
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field


class StepStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    SKIPPED   = "skipped"
    WAITING   = "waiting_approval"


class PlanStatus(str, Enum):
    CREATED   = "created"
    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    ROLLED_BACK = "rolled_back"
    AWAITING_APPROVAL = "awaiting_approval"
    ABORTED   = "aborted"


class SuccessCriterion(BaseModel):
    type:      Literal["world_model_query", "event_received", "metric_threshold"]
    check:     str
    timeout_s: int = 300


class RollbackStep(BaseModel):
    action:      str
    description: str
    parameters:  dict[str, Any] = Field(default_factory=dict)


class PlanStep(BaseModel):
    step_id:          str
    action:           str
    description:      str
    parameters:       dict[str, Any]       = Field(default_factory=dict)
    depends_on:       list[str]            = Field(default_factory=list)
    timeout_s:        int                  = 60
    on_failure:       Literal["rollback", "continue", "abort"] = "rollback"
    is_approval_gate: bool                 = False
    status:           StepStatus           = StepStatus.PENDING
    started_at:       datetime | None     = None
    completed_at:     datetime | None     = None
    result:           dict[str, Any]       = Field(default_factory=dict)
    error:            str | None          = None


class Plan(BaseModel):
    plan_id:          str = Field(
        default_factory=lambda: f"plan_{uuid.uuid4().hex[:10]}"
    )
    decision_id:      str    # links to DecisionObject that generated this plan
    goal:             str
    preconditions:    list[str]          = Field(default_factory=list)
    steps:            list[PlanStep]     = Field(default_factory=list)
    success_criteria: list[SuccessCriterion] = Field(default_factory=list)
    rollback_plan:    list[RollbackStep] = Field(default_factory=list)
    approval_gates:   list[str]         = Field(default_factory=list)
    status:           PlanStatus        = PlanStatus.CREATED
    strategy:         str               = "rule_based"
    agent_id:         str | None        = None
    created_at:       datetime          = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    completed_at:     datetime | None  = None

    def ready_steps(self) -> list[PlanStep]:
        """Return steps whose dependencies are all SUCCEEDED and are PENDING."""
        succeeded = {s.step_id for s in self.steps if s.status == StepStatus.SUCCEEDED}
        return [
            s for s in self.steps
            if s.status == StepStatus.PENDING
            and all(dep in succeeded for dep in s.depends_on)
        ]

    def is_complete(self) -> bool:
        return all(
            s.status in (StepStatus.SUCCEEDED, StepStatus.SKIPPED)
            for s in self.steps
        )
