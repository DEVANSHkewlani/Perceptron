"""
Database Normalizer  (source_type: database)
=============================================
Two ingestion paths:

PATH A — Prometheus exporter:
  postgres_exporter → Prometheus → Alertmanager → Perception API → this normalizer.

PATH B — Direct query adapter:
  DatabaseAdapter runs SQL against pg_stat_activity, pg_stat_replication, pg_locks.
  Produces a dict → this normalizer.

Both converge on the same event_type vocabulary from event_types.py.
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


# ─────────────────────────────────────────────
# DATABASE ALERT → EVENT_TYPE MAP
# Keys match Prometheus alertname labels from postgres_exporter rules
# ─────────────────────────────────────────────

DB_ALERT_MAP: dict[str, tuple[str, Severity, float]] = {
    # Connection pool
    "PostgresConnectionsHigh":       ("connection_pool_high",       Severity.MEDIUM,   0.97),
    "PostgresConnectionsExhausted":  ("connection_pool_exhausted",  Severity.CRITICAL, 0.98),
    "PostgresIdleInTransaction":     ("connection_leak_detected",   Severity.HIGH,     0.95),
    "PostgresMaxConnectionsReached": ("max_connections_reached",    Severity.CRITICAL, 0.99),

    # Replication
    "PostgresReplicationLagHigh":    ("replication_lag_high",       Severity.HIGH,     0.98),
    "PostgresReplicationBroken":     ("replication_broken",         Severity.CRITICAL, 0.99),

    # Performance
    "PostgresSlowQueryRate":         ("slow_query_detected",        Severity.MEDIUM,   0.96),
    "PostgresQueryLatencyHigh":      ("query_latency_spike",        Severity.HIGH,     0.97),
    "PostgresDeadlock":              ("deadlock_detected",          Severity.HIGH,     0.99),
    "PostgresLockWaitTimeout":       ("lock_wait_timeout",          Severity.MEDIUM,   0.96),

    # Storage
    "PostgresTableSizeGrowthAlert":  ("table_size_growth_alert",    Severity.MEDIUM,   0.95),
    "PostgresVacuumNotRunning":      ("vacuum_not_running",         Severity.MEDIUM,   0.94),
    "PostgresDiskLow":               ("disk_space_for_db_low",      Severity.HIGH,     0.98),

    # Cache (Redis)
    "RedisCacheHitRatioLow":         ("cache_hit_ratio_dropped",    Severity.MEDIUM,   0.96),
    "RedisEvictionSpike":            ("cache_eviction_spike",       Severity.HIGH,     0.95),
    "RedisMemoryHigh":               ("memory_pressure_high",       Severity.HIGH,     0.97),
}


class DatabaseNormalizer(BaseNormalizer):
    """
    Normalizes Prometheus Alertmanager payloads for database alerts
    AND direct query adapter results into CognitiveEvents.
    """

    source_type = SourceType.DATABASE

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        # Detect if this is a Prometheus alert or a direct query result
        if "labels" in raw_input:
            return self._normalize_alert(raw_input, source_id)
        return self._normalize_direct(raw_input, source_id)

    def _normalize_alert(
        self, raw_input: dict, source_id: str
    ) -> CognitiveEvent:
        """Normalize a Prometheus Alertmanager payload."""
        labels      = raw_input.get("labels", {})
        annotations = raw_input.get("annotations", {})
        status      = raw_input.get("status", "firing")
        starts_at   = raw_input.get("startsAt")

        alert_name = labels.get("alertname", "")
        if not alert_name:
            raise ValueError("Database alert missing alertname label")

        if alert_name in DB_ALERT_MAP:
            event_type, severity, confidence = DB_ALERT_MAP[alert_name]
        else:
            event_type = "slow_query_detected"
            severity   = Severity.MEDIUM
            confidence = 0.80

        if status == "resolved":
            event_type = event_type + "_resolved"
            severity   = Severity.INFO

        db_name = labels.get(
            "datname",
            labels.get(
                "database",
                labels.get("instance", source_id).split(":")[0],
            ),
        )
        entity_refs = self._dedupe_refs([source_id, f"db:{db_name}"])

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "alert_name": alert_name,
                "status":     status,
                "database":   db_name,
                "summary":    annotations.get("summary", ""),
                "value":      annotations.get("value", ""),
                "labels":     labels,
            },
            entity_refs=entity_refs,
            confidence=confidence,
            tags=["database", db_name, status],
            timestamp=self._parse_timestamp(starts_at),
        )

    def _normalize_direct(
        self, raw_input: dict, source_id: str
    ) -> CognitiveEvent:
        """Normalize a direct query adapter result."""
        event_type = raw_input.get("event_type", "slow_query_detected")
        severity   = Severity(raw_input.get("severity", "medium"))
        confidence = raw_input.get("confidence", 0.95)
        db_name    = raw_input.get("database", source_id.replace("db:", ""))

        entity_refs = self._dedupe_refs([source_id, f"db:{db_name}"])

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                k: v
                for k, v in raw_input.items()
                if k not in ("event_type", "severity", "confidence", "source_id")
            },
            entity_refs=entity_refs,
            confidence=confidence,
            tags=["database", db_name],
        )
