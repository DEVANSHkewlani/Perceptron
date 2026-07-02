"""
Unified Event Schema
====================
The single contract every component in the cognitive architecture speaks.
Every source — logs, metrics, APIs, databases, queues, files, user events,
browser events, security events, sensor events, agent events — produces
exactly one of these.

Build order: THIS FILE FIRST. Nothing else before this.
"""

from __future__ import annotations

import uuid
import warnings
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ─────────────────────────────────────────────
# ENUMS  (controlled vocabularies)
# ─────────────────────────────────────────────

class SourceType(str, Enum):
    """
    What kind of signal source produced this event.
    Maps directly to which adapter/normalizer pipeline was used.
    """
    LOG            = "log"            # Server-side log lines
    METRIC         = "metric"         # Prometheus / numeric measurements
    API            = "api"            # HTTP endpoint health + latency
    QUEUE          = "queue"          # Message broker state
    FILE           = "file"           # Filesystem changes
    USER_EVENT     = "user_event"     # User interaction signals
    DATABASE       = "database"       # DB internals (queries, pool, replication)
    SENSOR         = "sensor"         # Physical / IoT sensors
    AGENT_EVENT    = "agent_event"    # Cognitive sub-agent decisions
    BROWSER_EVENT  = "browser_event"  # Client-side JS errors, Web Vitals
    SECURITY_EVENT = "security_event" # WAF, brute-force, geo anomalies


class Severity(str, Enum):
    """
    How urgent is this event.
    Used by: memory router, situation assessor, world model anomaly registry.
    """
    CRITICAL = "critical"  # Immediate action required. Service down, DDoS, data loss.
    HIGH     = "high"      # Significant degradation. Latency spike, pool exhaustion.
    MEDIUM   = "medium"    # Noteworthy deviation. Slow queries, elevated error rate.
    LOW      = "low"       # Minor issue. Single retries, brief spikes that self-resolved.
    INFO     = "info"      # Normal operational signal. Heartbeat, deployment success.


# ─────────────────────────────────────────────
# THE SCHEMA
# ─────────────────────────────────────────────

class CognitiveEvent(BaseModel):
    """
    The unified event envelope. Every signal source produces exactly this.

    CRITICAL FIELDS (what the rest of the architecture depends on):
    - event_type  : the semantic label. This is what reasoning queries on.
    - entity_refs : links this event to knowledge graph nodes.
    - confidence  : how certain is the normalizer about its interpretation.

    Without event_type and entity_refs populated correctly:
    - The world model cannot update
    - The knowledge graph cannot link
    - Reasoning is blind
    """

    # ── Identity ──────────────────────────────────────────────────────────
    event_id: str = Field(
        default="",   # will be set by model_validator below
        description="Deterministic event ID derived from source + type + timestamp.",
        examples=["evt_a1b2c3d4e5f6"],
    )

    # ── Timing ────────────────────────────────────────────────────────────
    timestamp: datetime = Field(
        description=(
            "When the event ACTUALLY OCCURRED in the environment. "
            "Not when it was processed. Use the source's own timestamp "
            "when available. ISO 8601 with timezone required."
        )
    )
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description=(
            "When the perception layer received and processed this event. "
            "Gap between timestamp and ingested_at = processing lag. "
            "Monitor this — growing lag means your adapter is falling behind."
        ),
    )

    # ── Source identification ──────────────────────────────────────────────
    source_type: SourceType = Field(
        description="Which signal class produced this. Determines which normalizer ran."
    )
    source_id: str = Field(
        description=(
            "The specific source instance. Must match entity namespace: "
            "'svc:auth-service', 'db:postgres-primary', 'usr:user-123', "
            "'sensor:temp-node-01'. This is how perception links to the graph."
        )
    )

    # ── Semantics (THE MOST IMPORTANT FIELDS) ─────────────────────────────
    event_type: str = Field(
        description=(
            "Semantic label for what happened. This is the normalized vocabulary. "
            "Examples: 'database_connection_timeout', 'cpu_spike', 'lcp_poor'. "
            "Always snake_case. Always from event_types.py vocabulary."
        )
    )
    severity: Severity = Field(
        description="How urgent. Used by memory router and situation assessor."
    )

    # ── Content ───────────────────────────────────────────────────────────
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Source-specific structured data. Always a dict, never a raw string. "
            "The normalizer's job is to structure this from raw input."
        ),
    )

    # ── Graph linkage (CRITICAL) ───────────────────────────────────────────
    entity_refs: list[str] = Field(
        default_factory=list,
        description=(
            "Entity IDs from the knowledge graph this event relates to. "
            "Every entity mentioned in the event must appear here. "
            "Format: 'svc:name', 'db:name', 'usr:id', 'agent:id', 'queue:name', "
            "'sensor:id', 'file:path', 'security:ip-x-x-x-x'."
        ),
    )

    # ── Reliability ────────────────────────────────────────────────────────
    confidence: float = Field(
        ge=0.0, le=1.0,
        description=(
            "How certain is the normalizer about this interpretation. "
            "Prometheus threshold crossing = 0.98 (deterministic). "
            "event_type inferred from unstructured log = 0.75 (regex match). "
            "Downstream components weight everything by this score."
        ),
    )

    # ── Routing ────────────────────────────────────────────────────────────
    tags: list[str] = Field(
        default_factory=list,
        description=(
            "Flexible routing labels. Used by memory router to decide which "
            "memory layers receive this event. "
            "Examples: ['production', 'database', 'latency', 'customer-facing']"
        ),
    )

    agent_id: str | None = Field(
        default=None,
        description="The ID of the cognitive agent associated with this event."
    )

    # ── Validators ─────────────────────────────────────────────────────────

    @field_validator("source_id")
    @classmethod
    def validate_source_id_namespace(cls, v: str) -> str:
        """source_id must follow the entity namespace convention."""
        valid_prefixes = (
            "svc:", "db:", "usr:", "metric:", "queue:", "file:",
            "agent:", "sensor:", "browser:", "security:", "ext:",
        )
        if not any(v.startswith(p) for p in valid_prefixes):
            raise ValueError(
                f"source_id '{v}' must use entity namespace prefix. "
                f"Valid prefixes: {valid_prefixes}. "
                f"Example: 'svc:auth-service', 'db:postgres-primary'"
            )
        return v

    @field_validator("event_type")
    @classmethod
    def validate_event_type_format(cls, v: str) -> str:
        """event_type must be lowercase snake_case."""
        if " " in v:
            raise ValueError(
                f"event_type '{v}' must be snake_case (no spaces). "
                f"Use: 'database_connection_timeout' not 'database connection timeout'"
            )
        if v != v.lower():
            raise ValueError(
                f"event_type '{v}' must be lowercase snake_case. "
                f"Use: 'cpu_spike' not 'CPU_Spike'"
            )
        return v

    @field_validator("entity_refs")
    @classmethod
    def validate_entity_refs(cls, refs: list[str]) -> list[str]:
        """All entity refs must follow namespace convention."""
        valid_prefixes = (
            "svc:", "db:", "usr:", "metric:", "queue:", "file:",
            "agent:", "sensor:", "browser:", "security:", "ext:",
        )
        for ref in refs:
            if not any(ref.startswith(p) for p in valid_prefixes):
                raise ValueError(
                    f"entity_ref '{ref}' must use namespace prefix. "
                    f"Valid: {valid_prefixes}"
                )
        return refs

    @model_validator(mode="after")
    def warn_empty_entity_refs(self) -> "CognitiveEvent":
        """
        entity_refs should never be empty for non-info events.
        Empty entity_refs means this event cannot update the knowledge graph.
        """
        if not self.entity_refs and self.severity not in (Severity.INFO, "info"):
            warnings.warn(
                f"Event '{self.event_type}' (severity={self.severity}) "
                f"has empty entity_refs. It cannot update the knowledge graph. "
                f"Add entity_refs to enable relational reasoning.",
                UserWarning,
                stacklevel=2,
            )
        return self

    @model_validator(mode="after")
    def set_deterministic_id(self) -> "CognitiveEvent":
        """
        Generate a deterministic event_id from the natural key.
        Same physical event reprocessed = same event_id every time.
        """
        if not self.event_id:
            NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
            natural_key = f"{self.source_id}:{self.event_type}:{self.timestamp.isoformat()}"
            det_uuid = uuid.uuid5(NAMESPACE, natural_key).hex[:12]
            self.event_id = f"evt_{det_uuid}"
        return self

    model_config = {
        "use_enum_values": True,
        "json_encoders": {datetime: lambda v: v.isoformat()},
    }


# ─────────────────────────────────────────────
# PERCEPTION FAILURE EVENT
# ─────────────────────────────────────────────

class PerceptionFailure(BaseModel):
    """
    When a normalizer cannot produce a valid CognitiveEvent, it emits this.
    Never silently drop data — always record failures.

    Published to: cognitive.perception_failures Kafka topic.
    The perception health dashboard monitors these. High failure rates on
    a source indicate: bad schema, changed log format, or adapter bug.
    """
    failure_id:    str      = Field(default_factory=lambda: f"fail_{uuid.uuid4().hex[:8]}")
    failed_at:     datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_type:   str
    source_id:     str
    raw_input:     str      = Field(description="The raw input that could not be normalised.")
    error_message: str
    normalizer:    str      = Field(description="Which normalizer class failed.")
    stacktrace:    str | None = None
