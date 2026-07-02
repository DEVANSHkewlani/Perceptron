"""
Log Normalizer  (source_type: log)
===================================
Transforms raw log lines — from Fluent Bit, Filebeat, or direct tail —
into CognitiveEvents with extracted event_type and entity_refs.

The hardest normalizer because logs are unstructured.
Confidence is always lower (0.65–0.85) than structured sources.

FLOW:
  1. Adapter (Fluent Bit) tails the log file and sends JSON to Kafka
  2. This normalizer consumes from the Kafka topic
  3. Tries JSON parsing first (structured logs), then regex (plaintext)
  4. Extracts event_type by matching patterns
  5. Extracts entity_refs by recognizing service names and DB names
  6. Emits a CognitiveEvent or a PerceptionFailure
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


# ─────────────────────────────────────────────
# ENTITY EXTRACTION HELPERS
# ─────────────────────────────────────────────

def _extract_db_refs(text: str) -> list[str]:
    """Find database names mentioned in log text."""
    refs: list[str] = []
    db_patterns = [
        r"(?:postgres|postgresql|mysql|mongo|redis)[-_]?(\w+)",
        r"database[:\s]+['\"]?(\w[\w-]*)",
        r"db[:\s]+['\"]?(\w[\w-]*)",
    ]
    for pat in db_patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            name = m.group(0).lower().replace(" ", "-").replace(":", "-")
            refs.append(f"db:{name}")
    return list(set(refs))


def _extract_service_refs(text: str) -> list[str]:
    """Find service names mentioned in log text."""
    refs: list[str] = []
    svc_patterns = [
        r"service[:\s]+['\"]?([\w-]+)",
        r"upstream[:\s]+['\"]?([\w-]+)",
    ]
    for pat in svc_patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            refs.append(f"svc:{m.group(1).lower()}")
    return list(set(refs))


# ─────────────────────────────────────────────
# PATTERN LIBRARY
# (regex, event_type, severity, confidence, extract_entities_fn)
# ─────────────────────────────────────────────

LOG_PATTERNS: list[tuple] = [
    # ── Connection pool exhaustion (ShopCore chaos) ──────────────────
    (
        re.compile(
            r"(?:connection|conn).*?pool.*?exhaust|pool.*?exhausted",
            re.IGNORECASE,
        ),
        "connection_pool_exhausted",
        Severity.CRITICAL,
        0.90,
        _extract_service_refs,
    ),
    # ── Database connection/timeout ──────────────────────────────────
    (
        re.compile(
            r"connection.*?(?:timed? ?out|refused|failed|error)|"
            r"(?:pool|connection).*?exhaust|"
            r"too many connections",
            re.IGNORECASE,
        ),
        "database_connection_timeout",
        Severity.HIGH,
        0.82,
        _extract_db_refs,
    ),
    # ── Database query timeout ───────────────────────────────────────
    (
        re.compile(
            r"query.*?(?:timed? ?out|exceeded|slow)|slow.?query", re.IGNORECASE
        ),
        "database_query_timeout",
        Severity.MEDIUM,
        0.80,
        _extract_db_refs,
    ),
    # ── JWT / auth failures ──────────────────────────────────────────
    (
        re.compile(
            r"jwt.*?(?:invalid|expired|failed|error)|"
            r"token.*?(?:invalid|expired|rejected)|"
            r"authentication.*?fail",
            re.IGNORECASE,
        ),
        "jwt_validation_failure",
        Severity.MEDIUM,
        0.85,
        lambda text: [],
    ),
    # ── HTTP 500 ─────────────────────────────────────────────────────
    (
        re.compile(
            r"\b500\b.*?(?:error|internal)|internal.server.error", re.IGNORECASE
        ),
        "http_500_error",
        Severity.HIGH,
        0.90,
        _extract_service_refs,
    ),
    # ── Permission denied ────────────────────────────────────────────
    (
        re.compile(
            r"permission.denied|access.denied|forbidden|unauthorized",
            re.IGNORECASE,
        ),
        "permission_denied",
        Severity.MEDIUM,
        0.85,
        lambda text: [],
    ),
    # ── Memory / OOM ─────────────────────────────────────────────────
    (
        re.compile(
            r"out.of.memory|oom|heap.space|memory.leak|"
            r"java\.lang\.OutOfMemory",
            re.IGNORECASE,
        ),
        "memory_leak_warning",
        Severity.HIGH,
        0.80,
        lambda text: [],
    ),
    # ── Service crash / exit ─────────────────────────────────────────
    (
        re.compile(
            r"(?:process|service|worker).{0,20}(?:crash|exit|kill|died)",
            re.IGNORECASE,
        ),
        "service_crashed",
        Severity.CRITICAL,
        0.78,
        lambda text: [],
    ),
    # ── Rate limit ───────────────────────────────────────────────────
    (
        re.compile(r"rate.?limit|too.many.requests|429", re.IGNORECASE),
        "rate_limit_hit",
        Severity.LOW,
        0.88,
        lambda text: [],
    ),
    # ── Deployment events ────────────────────────────────────────────
    (
        re.compile(
            r"deployment.{0,20}(?:start|begin|initiated)", re.IGNORECASE
        ),
        "deployment_started",
        Severity.INFO,
        0.90,
        lambda text: [],
    ),
    (
        re.compile(
            r"deployment.{0,20}(?:complet|finish|success)", re.IGNORECASE
        ),
        "deployment_completed",
        Severity.INFO,
        0.90,
        lambda text: [],
    ),
    (
        re.compile(
            r"deployment.{0,20}(?:fail|error|abort)|rollback", re.IGNORECASE
        ),
        "deployment_failed",
        Severity.HIGH,
        0.85,
        lambda text: [],
    ),
    # ── Dependency call failure ──────────────────────────────────────
    (
        re.compile(
            r"(?:downstream|upstream|dependency).{0,20}(?:fail|error|timeout|unavailable)",
            re.IGNORECASE,
        ),
        "dependency_call_failed",
        Severity.HIGH,
        0.80,
        _extract_service_refs,
    ),
    # ── Disk write failure ───────────────────────────────────────────
    (
        re.compile(
            r"disk.{0,10}(?:write|full|space)|no.space.left|ENOSPC",
            re.IGNORECASE,
        ),
        "disk_write_failed",
        Severity.HIGH,
        0.85,
        lambda text: [],
    ),
    # ── Unhandled exception (catch-all — must be last) ───────────────
    (
        re.compile(
            r"unhandled.exception|uncaught.error|stacktrace|traceback",
            re.IGNORECASE,
        ),
        "unhandled_exception",
        Severity.HIGH,
        0.75,
        lambda text: [],
    ),
]


# ─────────────────────────────────────────────
# LEVEL → SEVERITY MAPPING
# ─────────────────────────────────────────────

LEVEL_SEVERITY: dict[str, Severity] = {
    "fatal":    Severity.CRITICAL,
    "critical": Severity.CRITICAL,
    "error":    Severity.HIGH,
    "err":      Severity.HIGH,
    "warn":     Severity.MEDIUM,
    "warning":  Severity.MEDIUM,
    "info":     Severity.INFO,
    "debug":    Severity.INFO,
    "trace":    Severity.INFO,
}


class LogNormalizer(BaseNormalizer):
    """
    Normalizes log lines from any application log source.

    INPUT: A dict from Fluent Bit (parsed JSON log) or a plain string.
    Fluent Bit sends structured JSON like:
        {
            "timestamp": "2024-01-15T14:23:11Z",
            "level": "ERROR",
            "message": "connection to postgres-primary timed out after 5000ms",
            "service": "auth-service",
            "pod": "auth-service-abc123",
        }
    Or it may send raw parsed key-value pairs from plaintext logs.
    """

    source_type = SourceType.LOG

    def _normalize(
        self, raw_input: dict | str, source_id: str
    ) -> CognitiveEvent:
        # ── 1. Parse raw input ───────────────────────────────────────
        if isinstance(raw_input, str):
            log = {"message": raw_input, "level": "error"}
        else:
            log = raw_input

        message: str = log.get("message", log.get("msg", log.get("log", "")))
        level: str = (
            log.get("level", log.get("severity", log.get("lvl", "info")))
            .lower()
        )
        service: str = log.get(
            "service", log.get("app", source_id.replace("svc:", ""))
        )
        ts_str: str | None = log.get(
            "timestamp", log.get("time", log.get("@timestamp"))
        )

        if not message:
            raise ValueError("Log entry has no message field — cannot normalize")

        # ── 2. Determine base severity from log level ────────────────
        base_severity = LEVEL_SEVERITY.get(level, Severity.INFO)

        # ── 3. Match against pattern library ─────────────────────────
        event_type = "unhandled_exception"  # fallback
        severity = base_severity
        confidence = 0.65  # low confidence for unmatched logs
        extra_refs: list[str] = []

        for pattern, evt_type, evt_severity, evt_confidence, extract_fn in LOG_PATTERNS:
            if pattern.search(message):
                event_type = evt_type
                severity = evt_severity
                confidence = evt_confidence
                extra_refs = extract_fn(message)
                break

        # If nothing matched but it's an error-level log, still emit
        if (
            base_severity in (Severity.CRITICAL, Severity.HIGH)
            and event_type == "unhandled_exception"
        ):
            confidence = 0.65

        # ── 4. Build entity refs ─────────────────────────────────────
        entity_refs = self._dedupe_refs(
            [f"svc:{service}"] + extra_refs
        )

        # ── 5. Build payload ─────────────────────────────────────────
        payload = {
            "message": message[:500],
            "level": level,
            "service": service,
            # Include extra fields (pod name, request ID, etc.)
            **{
                k: v
                for k, v in log.items()
                if k
                not in (
                    "message", "msg", "log", "level", "severity",
                    "service", "app", "timestamp", "time", "@timestamp",
                )
                and isinstance(v, (str, int, float, bool))
            },
        }

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload=payload,
            entity_refs=entity_refs,
            confidence=confidence,
            tags=["log", service, level],
            timestamp=self._parse_timestamp(ts_str),
        )
