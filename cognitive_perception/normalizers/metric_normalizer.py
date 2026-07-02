"""
Metric Normalizer  (source_type: metric)
=========================================
Transforms Prometheus Alertmanager webhook payloads into CognitiveEvents.
Highest-confidence events in the system (0.95–0.99) because
Prometheus threshold rules are deterministic.

FLOW:
  1. You define alerting rules in prometheus.yml (thresholds)
  2. When a rule fires, Prometheus sends a POST to Alertmanager
  3. Alertmanager routes it to your Perception API webhook
  4. This normalizer transforms the Alertmanager payload → CognitiveEvent
"""

from __future__ import annotations

import re
from datetime import datetime

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


# ─────────────────────────────────────────────
# ALERT NAME → (event_type, severity, confidence)
# ─────────────────────────────────────────────

ALERT_MAP: dict[str, tuple[str, Severity, float]] = {
    # CPU
    "HighCPUUsage":           ("cpu_spike",              Severity.HIGH,     0.98),
    "SustainedHighCPU":       ("cpu_sustained_high",     Severity.HIGH,     0.98),
    "CPUReturned":            ("cpu_returned_normal",    Severity.INFO,     0.98),

    # Memory
    "HighMemoryPressure":     ("memory_pressure_high",   Severity.HIGH,     0.97),
    "MemoryExhaustion":       ("memory_exhaustion",      Severity.CRITICAL, 0.97),
    "HighSwapUsage":          ("swap_usage_high",        Severity.MEDIUM,   0.96),

    # Disk
    "HighDiskUsage":          ("disk_usage_high",        Severity.MEDIUM,   0.98),
    "DiskIOSaturation":       ("disk_io_saturation",     Severity.MEDIUM,   0.96),
    "DiskFull":               ("disk_full",              Severity.CRITICAL, 0.99),

    # Network
    "NetworkThroughputSpike": ("network_throughput_spike", Severity.MEDIUM, 0.95),
    "PacketLossDetected":     ("packet_loss_detected",   Severity.HIGH,     0.94),

    # Process / runtime
    "HighThreadCount":        ("thread_count_spike",     Severity.MEDIUM,   0.96),
    "FDLimitApproaching":     ("fd_limit_approaching",   Severity.HIGH,     0.97),
    "ExcessiveGCPause":       ("gc_pause_excessive",     Severity.HIGH,     0.96),
    "HighGCFrequency":        ("gc_frequency_high",      Severity.MEDIUM,   0.95),

    # Container / Kubernetes
    "ContainerOOMKilled":     ("container_oom_killed",   Severity.CRITICAL, 0.99),
    "ContainerRestart":       ("container_restart",      Severity.HIGH,     0.99),
    "ContainerCPUThrottled":  ("container_cpu_throttled", Severity.MEDIUM,  0.96),
    "PodPendingTooLong":      ("pod_pending_too_long",   Severity.HIGH,     0.97),
    "NodeNotReady":           ("node_not_ready",         Severity.CRITICAL, 0.99),
    "KafkaConsumerLagHigh":    ("consumer_lag_critical",   Severity.CRITICAL, 0.99),

    # ShopCore gateway / product alerts
    "ShopCoreGatewayLatencyHigh": ("api_latency_spike",    Severity.HIGH,     0.98),
    "ShopCoreGatewayErrorRate":   ("service_health_degraded", Severity.HIGH,  0.97),
    "ShopCoreServiceDown":        ("service_unreachable",  Severity.CRITICAL, 0.99),
    "ShopCoreProductPoolExhausted": ("connection_pool_exhausted", Severity.CRITICAL, 0.98),
}


PROM_SEVERITY: dict[str, Severity] = {
    "critical": Severity.CRITICAL,
    "high":     Severity.HIGH,
    "warning":  Severity.MEDIUM,
    "info":     Severity.INFO,
    "page":     Severity.CRITICAL,
}


class MetricNormalizer(BaseNormalizer):
    """
    Normalizes Prometheus Alertmanager webhook payloads.

    One webhook POST can contain multiple alerts in the 'alerts' array.
    Call normalize() once per alert (split in the adapter / API layer).
    """

    source_type = SourceType.METRIC

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        """
        raw_input: a single alert dict from Alertmanager's 'alerts' array.
        """
        labels      = raw_input.get("labels", {})
        annotations = raw_input.get("annotations", {})
        status      = raw_input.get("status", "firing")
        starts_at   = raw_input.get("startsAt")

        # ── 1. Extract alert name ────────────────────────────────────
        alert_name = labels.get("alertname", "")
        if not alert_name:
            raise ValueError("Alertmanager payload missing 'alertname' label")

        # ── 2. Look up event_type mapping ────────────────────────────
        if alert_name in ALERT_MAP:
            event_type, severity, confidence = ALERT_MAP[alert_name]
        else:
            event_type = self._alert_name_to_event_type(alert_name)
            severity = PROM_SEVERITY.get(
                labels.get("severity", "warning").lower(), Severity.MEDIUM
            )
            confidence = 0.90

        # If resolved, suffix the event type and downgrade severity
        if status == "resolved":
            event_type = (
                event_type
                .replace("_spike", "_resolved")
                .replace("_high", "_resolved")
                .replace("_exhaustion", "_resolved")
            )
            if not event_type.endswith("_resolved"):
                event_type += "_resolved"
            severity = Severity.INFO

        # ── 3. Extract entity refs ───────────────────────────────────
        entity_refs: list[str] = []
        if service := labels.get("service"):
            entity_refs.append(f"svc:{service}")
        if instance := labels.get("instance"):
            host = instance.split(":")[0]
            entity_refs.append(f"metric:{host}")
        if job := labels.get("job"):
            entity_refs.append(f"svc:{job}")
        if not entity_refs:
            entity_refs.append(source_id)
        entity_refs = self._dedupe_refs(entity_refs)

        # ── 4. Build payload ─────────────────────────────────────────
        payload = {
            "alert_name":  alert_name,
            "status":      status,
            "summary":     annotations.get("summary", ""),
            "description": annotations.get("description", ""),
            "value":       annotations.get(
                "value", annotations.get("current_value", "")
            ),
            "labels":      labels,
            "instance":    labels.get("instance", ""),
        }

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload=payload,
            entity_refs=entity_refs,
            confidence=confidence,
            tags=["metric", labels.get("job", ""), status],
            timestamp=self._parse_timestamp(starts_at),
        )

    @staticmethod
    def _alert_name_to_event_type(alert_name: str) -> str:
        """Convert CamelCase Prometheus alertname to snake_case event_type."""
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", alert_name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
