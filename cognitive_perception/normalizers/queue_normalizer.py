"""
Queue Normalizer  (source_type: queue)
=======================================
Converts queue metrics from any broker into CognitiveEvents.
Decoupled from broker specifics — receives a QueueMetrics dict.
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


class QueueNormalizer(BaseNormalizer):
    """
    Converts queue metrics into CognitiveEvents.

    raw_input is a dict with:
        source_id, broker_type, queue_name, consumer_group,
        consumer_lag, queue_depth, oldest_message_age_s,
        dead_letter_count, consumer_count, throughput_rate,
        lag_warn, lag_critical, dlq_warn, msg_age_warn_s, depth_warn
    """

    source_type = SourceType.QUEUE

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        lag         = raw_input.get("consumer_lag", 0)
        depth       = raw_input.get("queue_depth", 0)
        msg_age_s   = raw_input.get("oldest_message_age_s", 0)
        dlq         = raw_input.get("dead_letter_count", 0)
        consumers   = raw_input.get("consumer_count", 0)
        throughput  = raw_input.get("throughput_rate", 0.0)
        queue_name  = raw_input.get("queue_name", "unknown")
        broker_type = raw_input.get("broker_type", "kafka")

        lag_warn     = raw_input.get("lag_warn", 1000)
        lag_critical = raw_input.get("lag_critical", 10000)
        dlq_warn     = raw_input.get("dlq_warn", 10)
        age_warn     = raw_input.get("msg_age_warn_s", 300)
        depth_warn   = raw_input.get("depth_warn", 5000)

        # Determine the most severe condition (priority order)
        if consumers == 0 and depth > 0:
            event_type = "consumer_group_stopped"
            severity   = Severity.CRITICAL
            confidence = 0.96
        elif lag >= lag_critical:
            event_type = "consumer_lag_critical"
            severity   = Severity.CRITICAL
            confidence = 0.97
        elif dlq >= dlq_warn:
            event_type = "dead_letter_queue_growing"
            severity   = Severity.HIGH
            confidence = 0.97
        elif lag >= lag_warn:
            event_type = "consumer_lag_high"
            severity   = Severity.HIGH
            confidence = 0.96
        elif msg_age_s >= age_warn:
            event_type = "message_age_exceeded"
            severity   = Severity.MEDIUM
            confidence = 0.95
        elif depth >= depth_warn:
            event_type = "queue_depth_high"
            severity   = Severity.MEDIUM
            confidence = 0.96
        elif throughput == 0 and depth > 0:
            event_type = "consumer_group_stopped"
            severity   = Severity.HIGH
            confidence = 0.88
        else:
            # Everything looks healthy — emit info-level heartbeat
            event_type = "consumer_lag_resolved"
            severity   = Severity.INFO
            confidence = 0.90

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "broker_type":       broker_type,
                "queue":             queue_name,
                "consumer_group":    raw_input.get("consumer_group"),
                "consumer_lag":      lag,
                "queue_depth":       depth,
                "oldest_msg_age_s":  round(msg_age_s, 1),
                "dead_letter_count": dlq,
                "consumer_count":    consumers,
                "throughput_per_s":  round(throughput, 2),
            },
            entity_refs=self._dedupe_refs([source_id, f"queue:{queue_name}"]),
            confidence=confidence,
            tags=["queue", broker_type, queue_name],
        )
