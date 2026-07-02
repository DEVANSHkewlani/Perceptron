"""
Browser Event Normalizer  (source_type: browser_event)
======================================================
Normalizes browser environment signals:
- JavaScript exceptions (window.onerror)
- Promise rejections (window.onunhandledrejection)
- Core Web Vitals (PerformanceObserver)
- React/Next.js hydration failures
- Asset load failures
- Network errors from fetch/XHR
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


# Core Web Vitals thresholds (Google Good / Needs Improvement / Poor)
CWV_THRESHOLDS = {
    "LCP":  {"good": 2500, "poor": 4000},    # ms
    "FID":  {"good": 100,  "poor": 300},     # ms
    "CLS":  {"good": 0.10, "poor": 0.25},    # unitless score
    "TTFB": {"good": 800,  "poor": 1800},    # ms
    "FCP":  {"good": 1800, "poor": 3000},    # ms
    "INP":  {"good": 200,  "poor": 500},     # ms (replaces FID)
}


class BrowserEventNormalizer(BaseNormalizer):
    """
    Normalizes browser environment signals into CognitiveEvents.

    raw_input:
    {
        "type": "LCP" | "js_error" | "hydration_fail" | ...,
        "value": 3200,          # for Web Vitals
        "message": "...",       # for JS errors
        "stack": "...",
        "url": "https://...",
        "browser": "Chrome 120",
        "device": "mobile" | "desktop",
        "session_id": "...",
    }
    """

    source_type = SourceType.BROWSER_EVENT

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        evt_type   = raw_input.get("type", "")
        value      = raw_input.get("value", 0)
        message    = raw_input.get("message", "")
        url        = raw_input.get("url", "")
        browser    = raw_input.get("browser", "")
        device     = raw_input.get("device", "")
        session_id = raw_input.get("session_id", "unknown")
        ts_str     = raw_input.get("timestamp")

        # ── Core Web Vitals ──────────────────────────────────────────
        if evt_type in CWV_THRESHOLDS:
            thresholds = CWV_THRESHOLDS[evt_type]
            if value > thresholds["poor"]:
                event_type = f"{evt_type.lower()}_poor"
                severity   = Severity.HIGH
            elif value > thresholds["good"]:
                event_type = f"{evt_type.lower()}_needs_improvement"
                severity   = Severity.MEDIUM
            else:
                event_type = "page_load_slow"
                severity   = Severity.INFO
            confidence = 0.97

        # ── JavaScript errors ────────────────────────────────────────
        elif evt_type in ("js_error", "error"):
            if "ChunkLoadError" in message or "chunk" in message.lower():
                event_type = "js_chunk_load_failed"
                severity   = Severity.HIGH
            elif "Hydration" in message or "hydration" in message.lower():
                event_type = "hydration_failure"
                severity   = Severity.HIGH
            elif "TypeError" in message or "ReferenceError" in message:
                event_type = "js_exception"
                severity   = Severity.HIGH
            else:
                event_type = "js_exception"
                severity   = Severity.MEDIUM
            confidence = 0.90

        elif evt_type in ("unhandled_rejection", "promise_rejection"):
            event_type = "promise_rejection"
            severity   = Severity.MEDIUM
            confidence = 0.88

        elif evt_type in ("hydration_fail", "hydration_error"):
            event_type = "hydration_failure"
            severity   = Severity.HIGH
            confidence = 0.93

        elif evt_type in ("asset_fail", "resource_error"):
            event_type = "asset_load_failed"
            severity   = Severity.MEDIUM
            confidence = 0.90

        elif evt_type in ("network_error", "fetch_error", "api_fail"):
            event_type = "api_request_failed_client"
            severity   = Severity.MEDIUM
            confidence = 0.85

        elif evt_type in ("render_error", "white_screen"):
            event_type = "render_error"
            severity   = Severity.CRITICAL
            confidence = 0.88

        else:
            event_type = "js_exception"
            severity   = Severity.LOW
            confidence = 0.60

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "type":       evt_type,
                "value":      value,
                "message":    message[:300] if message else "",
                "stack":      raw_input.get("stack", "")[:500],
                "url":        url,
                "browser":    browser,
                "device":     device,
                "session_id": session_id,
            },
            entity_refs=[source_id, "svc:web-app"],
            confidence=confidence,
            tags=["browser", device, evt_type],
            timestamp=self._parse_timestamp(ts_str),
        )
