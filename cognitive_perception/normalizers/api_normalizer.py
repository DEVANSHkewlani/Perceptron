"""
API Normalizer  (source_type: api)
===================================
Transforms an API poll result dict into a CognitiveEvent.
Used by the APIAdapter after each HTTP health-check poll attempt.

The adapter handles the HTTP work.
This normalizer handles the semantic interpretation.
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


class APINormalizer(BaseNormalizer):
    """
    Normalizes API poll results into CognitiveEvents.

    raw_input from the adapter:
    {
        "url": "https://auth-service/health",
        "status_code": 200,          # None if connection failed
        "latency_ms": 145.3,
        "ok": True,
        "error": None,               # or exception message
        "ssl_days_remaining": 82,    # None if not HTTPS or couldn't check
        "response_body": {...},
        "latency_warn_ms": 500,
        "latency_high_ms": 2000,
    }
    """

    source_type = SourceType.API

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        url         = raw_input.get("url", "")
        status_code = raw_input.get("status_code")
        latency_ms  = raw_input.get("latency_ms", 0)
        ok          = raw_input.get("ok", False)
        error       = raw_input.get("error")
        ssl_days    = raw_input.get("ssl_days_remaining")

        warn_ms = raw_input.get("latency_warn_ms", 500)
        high_ms = raw_input.get("latency_high_ms", 2000)

        # ── Determine event_type and severity ────────────────────────
        if error or status_code is None:
            event_type = "service_unreachable"
            severity   = Severity.CRITICAL
            confidence = 0.97

        elif status_code >= 500:
            body_text = str(raw_input.get("response_body", "")).lower()
            if status_code == 503 and "pool" in body_text:
                event_type = "connection_pool_exhausted"
                severity   = Severity.CRITICAL
                confidence = 0.95
            else:
                event_type = "service_health_degraded"
                severity   = Severity.HIGH
                confidence = 0.97

        elif status_code >= 400:
            event_type = "api_4xx_rate_high"
            severity   = Severity.MEDIUM
            confidence = 0.90

        elif ssl_days is not None and ssl_days <= 0:
            event_type = "service_ssl_expired"
            severity   = Severity.CRITICAL
            confidence = 0.99

        elif ssl_days is not None and ssl_days <= 7:
            event_type = "service_ssl_expiring"
            severity   = Severity.HIGH
            confidence = 0.99

        elif ssl_days is not None and ssl_days <= 30:
            event_type = "service_ssl_expiring"
            severity   = Severity.MEDIUM
            confidence = 0.99

        elif latency_ms >= high_ms:
            event_type = "api_latency_spike"
            severity   = Severity.HIGH
            confidence = 0.97

        elif latency_ms >= warn_ms:
            event_type = "api_latency_spike"
            severity   = Severity.MEDIUM
            confidence = 0.96

        elif ok:
            event_type = "service_health_restored"
            severity   = Severity.INFO
            confidence = 0.97

        else:
            event_type = "service_health_degraded"
            severity   = Severity.MEDIUM
            confidence = 0.88

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "url":                url,
                "status_code":        status_code,
                "latency_ms":         round(latency_ms, 2),
                "ok":                 ok,
                "error":              error,
                "ssl_days_remaining": ssl_days,
            },
            entity_refs=[source_id],
            confidence=confidence,
            tags=raw_input.get("tags", []) + ["api"],
        )
