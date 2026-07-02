"""
RuleEngine — Fast Path Decision System
Matches known situation patterns against pre-defined rules.
Returns a DecisionObject without calling the LLM.
Rules are evaluated in priority order (highest first).
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable
from .schema import DecisionObject, RootCauseHypothesis


@dataclass
class Rule:
    name:        str
    priority:    int                        # higher = evaluated first
    condition:   Callable[[dict], bool]     # receives situation dict
    action:      str                        # must match actions.yaml key
    parameters:  Callable[[dict], dict]     # receives situation dict
    confidence:  float
    rationale:   str


def _top_anomaly(sit: dict) -> dict:
    anoms = sit.get("ranked_anomalies", [])
    return anoms[0] if anoms else {}

def _top_entity_id(sit: dict) -> str:
    a = _top_anomaly(sit)
    return a.get("entity_id", "")

def _severity(sit: dict) -> str:
    return _top_anomaly(sit).get("severity", "info")

def _event_type(sit: dict) -> str:
    return _top_anomaly(sit).get("event_type", "")


RULES: list[Rule] = [

    Rule(
        name="no_anomalies", priority=100,
        condition=lambda s: len(s.get("ranked_anomalies", [])) == 0,
        action="monitor_and_wait",
        parameters=lambda s: {"duration_m": 5},
        confidence=0.99,
        rationale="No active anomalies detected. Nothing to act on.",
    ),

    Rule(
        name="consumer_lag_critical", priority=90,
        condition=lambda s: any(
            a.get("event_type") in ("consumer_lag_critical", "consumer_lag_high")
            for a in s.get("ranked_anomalies", [])
        ),
        action="scale_consumer_group",
        parameters=lambda s: {
            "queue_id":       next((a.get("entity_id", "") for a in s.get("ranked_anomalies", []) if a.get("event_type") in ("consumer_lag_critical", "consumer_lag_high")), "queue:order-events"),
            "consumer_group": "notification-group",
            "instance_delta": 3,
        },
        confidence=0.95,
        rationale="Critical consumer lag. Scale notification consumers and unblock Kafka publishing.",
    ),

    Rule(
        name="service_unreachable", priority=89,
        condition=lambda s: _event_type(s) == "service_unreachable" and _severity(s) == "critical",
        action="restart_service",
        parameters=lambda s: {"service_id": _top_entity_id(s), "strategy": "rolling"},
        confidence=0.88,
        rationale="Service unreachable at critical severity. Restart the affected container.",
    ),

    Rule(
        name="api_latency_spike", priority=85,
        condition=lambda s: _event_type(s) == "api_latency_spike" and _severity(s) in ("high", "critical"),
        action="restart_service",
        parameters=lambda s: {"service_id": _top_entity_id(s) or "svc:api-gateway", "strategy": "rolling"},
        confidence=0.82,
        rationale="Gateway latency spike detected. Restart clears injected chaos latency.",
    ),

    Rule(
        name="slow_query_detected", priority=84,
        condition=lambda s: _event_type(s) == "slow_query_detected" and _severity(s) in ("high", "critical", "medium"),
        action="kill_slow_query",
        parameters=lambda s: {
            "db_id": _top_entity_id(s) or "db:shopcore-postgres",
            "pid": s.get("ranked_anomalies", [{}])[0].get("payload", {}).get("pid"),
        },
        confidence=0.86,
        rationale="Slow query detected. Terminate query or restart product service to clear chaos injection.",
    ),

    Rule(
        name="redis_memory_critical", priority=87,
        condition=lambda s: _event_type(s) in ("redis_memory_critical", "cache_eviction_spike") and _severity(s) in ("critical", "high"),
        action="restart_service",
        parameters=lambda s: {"service_id": "svc:user-service", "strategy": "rolling"},
        confidence=0.80,
        rationale="Redis memory pressure. Restart dependent services after chaos clears maxmemory.",
    ),

    Rule(
        name="service_health_degraded", priority=83,
        condition=lambda s: _event_type(s) == "service_health_degraded" and _severity(s) == "high",
        action="restart_service",
        parameters=lambda s: {"service_id": _top_entity_id(s) or "svc:api-gateway", "strategy": "rolling"},
        confidence=0.78,
        rationale="Elevated 5xx error rate. Restart gateway to clear chaos error injection.",
    ),

    Rule(
        name="connection_pool_exhausted", priority=88,
        condition=lambda s: _event_type(s) in (
            "connection_pool_exhausted", "max_connections_reached"
        ) and _severity(s) == "critical",
        action="restart_connection_pool",
        parameters=lambda s: {"service_id": _top_entity_id(s)},
        confidence=0.92,
        rationale="Pool exhausted at critical level. Restart is standard remediation.",
    ),

    Rule(
        name="disk_full", priority=95,
        condition=lambda s: _event_type(s) in ("disk_full", "disk_space_for_db_low"),
        action="escalate_to_human",
        parameters=lambda s: {
            "reason":  f"Disk full on {_top_entity_id(s)}. Requires manual intervention.",
            "urgency": "immediate",
        },
        confidence=0.99,
        rationale="Disk operations are irreversible. Always escalate to human operator.",
    ),

    Rule(
        name="high_cpu_isolated", priority=50,
        condition=lambda s: (
            _event_type(s) in ("cpu_spike", "cpu_sustained_high")
            and _top_anomaly(s).get("blast_radius_count", 1) == 0
        ),
        action="increase_log_verbosity",
        parameters=lambda s: {
            "service_id": _top_entity_id(s), "duration_m": 15
        },
        confidence=0.80,
        rationale="Isolated CPU spike. Increase observability before taking action.",
    ),
]


class RuleEngine:
    def __init__(self, playbook_success_rates: dict[str, float] | None = None):
        self.rules = sorted(RULES, key=lambda r: r.priority, reverse=True)
        # Optional: dict of action → success_rate loaded from procedural memory
        self._rates = playbook_success_rates or {}
        self.MIN_SUCCESS_RATE = 0.40  # rules below this go to LLM instead

    def evaluate(self, situation: dict) -> DecisionObject | None:
        """
        Evaluate rules in priority order.
        Gates rules whose historical success rate is too low.
        """
        for rule in self.rules:
            try:
                if not rule.condition(situation):
                    continue

                # Phase 10 gate: skip rules whose historical success rate is too low
                rate = self._rates.get(rule.action, 1.0)  # 1.0 = no failure data yet
                if rate < self.MIN_SUCCESS_RATE:
                    # Rule exists but has a bad track record — route to LLM
                    continue

                params = rule.parameters(situation)
                # Use actual success_rate as confidence instead of hardcoded value
                effective_confidence = min(rule.confidence, rate) if rate < 1.0 else rule.confidence
                return DecisionObject(
                    situation_assessment=f"Fast path rule matched: {rule.name} (rate={rate:.2f})",
                    root_cause_hypothesis=RootCauseHypothesis(
                        hypothesis=rule.rationale, confidence=effective_confidence
                    ),
                    recommended_action=rule.action,
                    action_parameters=params,
                    confidence=effective_confidence,
                    requires_human_approval=(effective_confidence < 0.65 or rule.action == "escalate_to_human"),
                    alternative_actions=[],
                    reasoning_trace=(
                        f"[FAST PATH] Rule '{rule.name}' (priority={rule.priority}) "
                        f"matched. Action: {rule.action}. Params: {params}. "
                        f"Historical success rate: {rate:.2f}. LLM call skipped."
                    ),
                )
            except Exception:
                continue   # rule evaluation error — try next rule
        return None   # no rule matched — caller must invoke LLM
