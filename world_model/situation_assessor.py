"""
SituationAssessor
Compresses the full world model into a situation summary for LLM reasoning.
Ranking formula:
  score = severity_weight * confidence * (1 + 0.2 * blast_radius_count) * recency_decay

No LLM is used here. This is deterministic rule-based ranking.
The output is a structured dict that becomes the system prompt context.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import WorldModel

SEVERITY_WEIGHT = {
    "critical": 10.0,
    "high":     5.0,
    "medium":   2.0,
    "low":      0.5,
    "info":     0.1,
}
RECENCY_HALF_LIFE_S = 300   # anomaly score halves every 5 minutes


class SituationAssessor:
    def __init__(self, world_model: "WorldModel"):
        self.wm = world_model

    async def assess(self, top_n: int = 5) -> dict:
        """
        Build the situation brief. Called by the Reasoning Engine before
        constructing its LLM prompt.
        Returns a structured dict with:
          - situation_summary: plain text header
          - ranked_anomalies: top-N anomalies with scores and blast radius
          - system_health: overall health counts
          - causal_insights: dependency chains for top anomalies
          - predictions: near-term forecasts for critical entities
          - uncertainty_notes: low-confidence observations
        """
        open_anomalies = self.wm.anomalies.get_open()
        now = datetime.now(timezone.utc)

        # Score every open anomaly
        scored = []
        for a in open_anomalies:
            # Blast radius count (sync call — already resolved in registry)
            blast = await self.wm.get_blast_radius(a.entity_id)
            blast_count = len(blast)

            # Recency decay
            opened = datetime.fromisoformat(a.opened_at.replace("Z", "+00:00"))
            age_s = (now - opened).total_seconds()
            recency = math.exp(-0.693 * age_s / RECENCY_HALF_LIFE_S)

            score = (
                SEVERITY_WEIGHT.get(a.severity, 1.0)
                * a.confidence
                * (1 + 0.2 * blast_count)
                * recency
            )
            scored.append((score, a, blast, blast_count))

        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:top_n]

        # Build causal insights for each top anomaly
        causal_insights = []
        for _, a, blast, _ in top:
            chain = await self.wm.get_causal_chain(a.entity_id)
            causal_insights.append({
                "entity_id":       a.entity_id,
                "anomaly_type":    a.event_type,
                "dependency_path": chain["dependency_chain"][:4],
                "correlated_with": chain["correlated_entities"][:3],
                "blast_radius": [
                    {"entity": r["entity_id"], "hop": r["hop_distance"],
                     "health": r["current_health"]}
                    for r in blast[:5]
                ],
            })

        # Health summary
        severity_counts = self.wm.anomalies.count_by_severity()
        all_entities = self.wm.entities.get_all()
        total = len(all_entities)
        healthy_count = sum(1 for e in all_entities if e.health_status == "healthy")

        # Predictions for top critical entity
        predictions = []
        if top:
            top_entity_id = top[0][1].entity_id
            top_event_type = top[0][1].event_type
            predictions = await self.wm.get_prediction(top_entity_id, top_event_type)

        # Low-confidence observations
        uncertain = [
            {"entity": e.entity_id, "confidence": e.confidence}
            for e in self.wm.entities.get_degraded()
            if e.confidence < 0.75
        ][:3]

        return {
            "situation_summary": self._build_summary(severity_counts, total, healthy_count),
            "ranked_anomalies": [
                {
                    "rank":       i + 1,
                    "score":      round(score, 3),
                    "anomaly_id": a.anomaly_id,
                    "entity_id":  a.entity_id,
                    "event_type": a.event_type,
                    "severity":   a.severity,
                    "confidence": a.confidence,
                    "opened_at":  a.opened_at,
                    "blast_radius_count": blast_count,
                }
                for i, (score, a, _, blast_count) in enumerate(top)
            ],
            "system_health": {
                "total_entities":   total,
                "healthy":          healthy_count,
                "degraded":         total - healthy_count,
                "by_severity":      severity_counts,
            },
            "causal_insights":   causal_insights,
            "predictions":       predictions[:3],
            "uncertainty_notes": uncertain,
            "assessed_at":       now.isoformat(),
        }

    @staticmethod
    def _build_summary(
        severity_counts: dict, total: int, healthy: int
    ) -> str:
        parts = []
        if severity_counts.get("critical", 0):
            parts.append(f"{severity_counts['critical']} CRITICAL anomaly(s)")
        if severity_counts.get("high", 0):
            parts.append(f"{severity_counts['high']} HIGH anomaly(s)")
        health_pct = round(healthy / total * 100) if total > 0 else 100
        summary = ", ".join(parts) if parts else "No active anomalies"
        return f"{summary}. {health_pct}% of {total} tracked entities healthy."
