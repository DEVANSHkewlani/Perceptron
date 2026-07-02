"""
ContextBuilder — fetches episodic experiences and activated playbooks
from the Memory API (Phase 4) to enrich the reasoning prompt.
"""
from __future__ import annotations
import httpx


class ContextBuilder:
    def __init__(self, memory_api_url: str = "http://localhost:8090"):
        self.memory_url = memory_api_url

    async def get_past_experiences(
        self, situation: dict, limit: int = 3
    ) -> list[dict]:
        """
        Semantic search for similar past situations.
        Uses the top anomaly event_type as the query.
        Returns 2–3 resolved experiences the LLM can reason from.
        """
        top_anomalies = situation.get("ranked_anomalies", [])
        if not top_anomalies:
            return []

        query = (
            top_anomalies[0].get("event_type", "") + " " +
            situation.get("situation_summary", "")[:120]
        )

        async with httpx.AsyncClient(timeout=8.0) as client:
            try:
                resp = await client.get(
                    f"{self.memory_url}/memory/episodic/search",
                    params={"q": query, "limit": limit, "resolved_only": "true"},
                )
                if resp.status_code == 200:
                    return resp.json().get("results", [])
            except Exception:
                pass
        return []

    async def get_activated_playbooks(
        self, event_type: str, severity: str
    ) -> list[dict]:
        """Retrieve playbooks that match the current situation trigger."""
        async with httpx.AsyncClient(timeout=8.0) as client:
            try:
                resp = await client.get(
                    f"{self.memory_url}/memory/procedural/playbooks",
                    params={"trigger_event": event_type, "trigger_severity": severity},
                )
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass
        return []

    def format_experiences(self, experiences: list[dict]) -> str:
        """Format past experiences into prompt-ready text."""
        if not experiences:
            return "No relevant past experiences found."
        lines = []
        for i, exp in enumerate(experiences, 1):
            decision = exp.get("decision", {})
            outcome  = exp.get("outcome", "unknown")
            lines.append(
                f"Experience {i}: {exp.get('event_type', 'unknown')} — "
                f"Action taken: {decision.get('recommended_action', 'N/A')} — "
                f"Outcome: {outcome}"
            )
        return "\n".join(lines)
