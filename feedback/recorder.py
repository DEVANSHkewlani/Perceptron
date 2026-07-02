"""
OutcomeRecorder — finalises episodic memory records.

When the ReasoningEngine (Phase 7) makes a decision, it stores an episodic
record with outcome: None. OutcomeRecorder patches that record using the Memory API.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
from .verifier import VerificationResult

log = logging.getLogger("feedback.recorder")


class OutcomeRecorder:
    def __init__(self, memory_url: str):
        self._memory_url = memory_url

    async def record(self, action_event: dict, verification: VerificationResult) -> None:
        """
        PATCH the episodic record whose payload.plan_id matches.
        """
        plan_id = action_event.get("payload", {}).get("plan_id")
        if not plan_id:
            return

        patch = {
            "outcome":             verification.outcome,
            "verified_at":         verification.verified_at,
            "anomalies_before":    verification.anomalies_before,
            "anomalies_after":     verification.anomalies_after,
            "entity_health_before": verification.entity_health_before,
            "entity_health_after":  verification.entity_health_after,
            "verification_delay_s": verification.delay_used_s,
            "verification_notes":   verification.notes,
            "resolved_at":         datetime.now(timezone.utc).isoformat(),
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                r = await client.patch(
                    f"{self._memory_url}/memory/episodic/by-plan/{plan_id}",
                    json=patch,
                )
                if r.status_code == 200:
                    log.info(
                        f"[OutcomeRecorder] Patched plan={plan_id} outcome={verification.outcome}"
                    )
                else:
                    log.warning(
                        f"[OutcomeRecorder] PATCH returned {r.status_code} for plan={plan_id}"
                    )
            except Exception as e:
                import traceback
                log.error(f"[OutcomeRecorder] Failed to patch record: {type(e)} {e}")
                traceback.print_exc()
