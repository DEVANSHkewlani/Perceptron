"""
User Behavior Normalizer  (source_type: user_event)
====================================================
Converts raw frontend SDK payloads into CognitiveEvents.
Infers user frustration, conversion state, and UX degradation
from raw DOM events.

A "click" is just a click.
Three clicks in 2 seconds on the same element is rage_click_detected.
A session that ends without checkout_completed is cart_abandoned.
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


class UserBehaviorNormalizer(BaseNormalizer):
    """
    Transforms frontend SDK events into semantically rich CognitiveEvents.

    NOTE: Session-level aggregation (detecting abandonment across events)
    requires working memory. This normalizer handles single-event interpretation.
    The world model handles cross-event patterns.
    """

    source_type = SourceType.USER_EVENT

    # raw_type → (event_type, severity, confidence, category)
    EVENT_MAP: dict[str, tuple[str, Severity, float, str]] = {
        "page_view":          ("page_viewed",            Severity.INFO,   0.95, "navigation"),
        "page_exit":          ("page_exit",              Severity.INFO,   0.90, "navigation"),
        "click":              ("page_viewed",            Severity.INFO,   0.80, "interaction"),
        "rage_click":         ("rage_click_detected",    Severity.MEDIUM, 0.92, "frustration"),
        "dead_click":         ("dead_click_detected",    Severity.LOW,    0.85, "frustration"),
        "scroll":             ("scroll_depth_reached",   Severity.INFO,   0.70, "engagement"),
        "session_start":      ("session_started",        Severity.INFO,   0.98, "session"),
        "session_end":        ("session_ended",          Severity.INFO,   0.95, "session"),
        "form_submit":        ("form_submitted",         Severity.INFO,   0.95, "conversion"),
        "form_error":         ("form_validation_failed", Severity.MEDIUM, 0.92, "conversion"),
        "form_server_error":  ("form_submission_error",  Severity.HIGH,   0.95, "conversion"),
        "cart_add":           ("cart_item_added",        Severity.INFO,   0.97, "conversion"),
        "cart_remove":        ("cart_item_added",        Severity.INFO,   0.90, "conversion"),
        "cart_abandon":       ("cart_abandoned",         Severity.HIGH,   0.90, "conversion"),
        "checkout_start":     ("checkout_started",       Severity.INFO,   0.97, "conversion"),
        "checkout_complete":  ("checkout_completed",     Severity.INFO,   0.99, "conversion"),
        "checkout_fail":      ("checkout_failed",        Severity.HIGH,   0.97, "conversion"),
        "checkout_error":     ("checkout_failed",        Severity.HIGH,   0.95, "conversion"),
        "signup_start":       ("signup_started",         Severity.INFO,   0.97, "conversion"),
        "signup_complete":    ("signup_completed",       Severity.INFO,   0.99, "conversion"),
        "signup_abandon":     ("signup_abandoned",       Severity.MEDIUM, 0.88, "conversion"),
        "search":             ("search_performed",       Severity.INFO,   0.95, "search"),
        "search_no_results":  ("search_zero_results",    Severity.MEDIUM, 0.95, "search"),
        "feature_click":      ("feature_used",           Severity.INFO,   0.90, "engagement"),
        "bounce":             ("session_duration_short", Severity.MEDIUM, 0.85, "engagement"),
    }

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        raw_type   = raw_input.get("event_type", raw_input.get("type", "click"))
        page       = raw_input.get("page", raw_input.get("url", "/"))
        session_id = raw_input.get("session_id", "unknown")
        user_id    = raw_input.get("user_id")
        ts_str     = raw_input.get("timestamp")
        context    = raw_input.get("context", {})

        # Look up semantic mapping
        if raw_type in self.EVENT_MAP:
            event_type, severity, confidence, category = self.EVENT_MAP[raw_type]
        else:
            event_type = raw_type if "_" in raw_type else "feature_used"
            severity   = Severity.INFO
            confidence = 0.70
            category   = "unknown"

        # Elevate severity for critical conversion failures
        if event_type in ("checkout_failed", "form_submission_error"):
            if str(context.get("error_code", "")).startswith("5"):
                severity = Severity.HIGH

        # Build entity refs
        entity_refs = [source_id]
        if user_id:
            entity_refs.append(f"usr:{user_id}")
        entity_refs.append("svc:web-app")
        entity_refs = self._dedupe_refs(entity_refs)

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "raw_type":   raw_type,
                "page":       page,
                "session_id": session_id,
                "user_id":    user_id,
                "category":   category,
                "element":    raw_input.get("element", ""),
                "context":    context,
            },
            entity_refs=entity_refs,
            confidence=confidence,
            tags=["user_event", category, page.split("?")[0][:30]],
            timestamp=self._parse_timestamp(ts_str),
        )
