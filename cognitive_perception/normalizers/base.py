"""
Base Normalizer
===============
Every source-specific normalizer inherits from this.
Enforces the contract: raw input in → CognitiveEvent out.
If it cannot produce a valid event, it produces a PerceptionFailure.
It NEVER silently drops data.

Rule: "Each normalizer is a pure function: raw input in, validated event out.
If it cannot produce a valid event, it emits a perception_failure instead."
"""

from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from ..schema.event import CognitiveEvent, PerceptionFailure, Severity, SourceType
from ..schema.event_types import is_known_event_type


class BaseNormalizer(ABC):
    """
    Abstract base class for all normalizers.

    Subclass this for each signal source. Implement `_normalize()`.
    Call `normalize()` from your adapter — it handles failure wrapping.

    Example:
        class AppLogNormalizer(BaseNormalizer):
            source_type = SourceType.LOG

            def _normalize(self, raw: dict, source_id: str) -> CognitiveEvent:
                ...  # raise freely — base class catches and wraps failures
    """

    # Subclasses MUST set this class variable
    source_type: SourceType

    def normalize(
        self, raw_input: Any, source_id: str
    ) -> CognitiveEvent | PerceptionFailure:
        """
        Public entry point. Always returns something — either a valid
        CognitiveEvent or a PerceptionFailure. Never raises an exception.
        """
        try:
            event = self._normalize(raw_input, source_id)

            # Warn if event_type is not in the known vocabulary
            if not is_known_event_type(event.event_type):
                # Resolved variants (ending in _resolved) are implicitly valid if their base event type exists
                is_resolved_variant = event.event_type.endswith("_resolved") and is_known_event_type(event.event_type[:-9])
                if not is_resolved_variant:
                    import warnings
                    warnings.warn(
                        f"Normalizer produced unknown event_type '{event.event_type}'. "
                        f"Add it to schema/event_types.py to track it in the vocabulary.",
                        UserWarning,
                    )

            return event

        except Exception as e:
            return PerceptionFailure(
                source_type=str(self.source_type),
                source_id=source_id,
                raw_input=str(raw_input)[:2000],   # cap at 2 KB
                error_message=str(e),
                normalizer=self.__class__.__name__,
                stacktrace=traceback.format_exc(),
            )

    @abstractmethod
    def _normalize(self, raw_input: Any, source_id: str) -> CognitiveEvent:
        """
        Transform raw source input into a CognitiveEvent.
        Raise any exception if normalization fails —
        the base class will catch it and produce a PerceptionFailure.
        """
        ...

    # ── Helper methods for subclasses ────────────────────────────────────

    def _parse_timestamp(
        self, ts_string: str | None, fallback: bool = True
    ) -> datetime:
        """
        Parse a timestamp string from a source.
        Falls back to now() if the source doesn't provide one (and fallback=True).
        """
        if ts_string is None:
            if fallback:
                return datetime.now(timezone.utc)
            raise ValueError("Timestamp is required but not provided")

        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(ts_string)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _build_event(
        self,
        source_id: str,
        event_type: str,
        severity: Severity,
        payload: dict,
        entity_refs: list[str],
        confidence: float,
        tags: list[str] | None = None,
        timestamp: datetime | None = None,
    ) -> CognitiveEvent:
        """
        Convenience builder. Handles timestamp defaulting.
        Also calculates telemetry/ingestion lag, auto-tags stale events, and
        inserts the lag value into the event payload.
        """
        now = datetime.now(timezone.utc)
        source_ts = timestamp or now
        
        # Calculate lag in seconds
        lag_seconds = (now - source_ts).total_seconds()
        
        # Enrich tags dynamically based on lag
        enriched_tags = list(tags or [])
        if lag_seconds > 60:
            enriched_tags.append("stale_event")
        if lag_seconds > 300:
            enriched_tags.append("very_stale")

        return CognitiveEvent(
            timestamp=source_ts,
            ingested_at=now,
            source_type=self.source_type,
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={**payload, "_ingestion_lag_s": round(lag_seconds, 3)},
            entity_refs=entity_refs,
            confidence=confidence,
            tags=enriched_tags,
        )

    def _dedupe_refs(self, refs: list[str]) -> list[str]:
        """Deduplicate entity refs while preserving order."""
        seen = set()
        return [r for r in refs if not (r in seen or seen.add(r))]
