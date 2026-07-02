from __future__ import annotations
import logging
import httpx
from datetime import datetime, timezone

log = logging.getLogger("conflict_resolver")

RISK_SCORE = {"low": 1, "medium": 2, "high": 3}


class ConflictResolver:
    def __init__(self, world_model_url: str, planning_url: str = "http://localhost:8094"):
        self._wm_url = world_model_url
        self._pl_url = planning_url

    async def resolve(self, conflict: dict) -> None:
        entity_id = conflict.get("entity_id")
        tasks     = conflict.get("tasks", [])

        if len(tasks) < 2:
            return

        log.warning(f"[Conflict] Entity={entity_id} has {len(tasks)} competing tasks")

        # Score each task: confidence * (1/risk_score) -> higher is preferred
        def score(t):
            risk_val = RISK_SCORE.get(t.get("risk", "high"), 3)
            return t.get("confidence", 0.5) / risk_val

        ranked = sorted(tasks, key=score, reverse=True)
        winner = ranked[0]
        losers = ranked[1:]

        # If winner action is reversible and loser is low-risk -> MERGE (sequential)
        can_merge = (
            winner.get("risk") in ("low", "medium") and
            all(l.get("risk") == "low" for l in losers)
        )

        async with httpx.AsyncClient(timeout=8.0) as client:
            if can_merge:
                log.info(f"[Conflict] MERGE: {winner['action']} then {losers[0]['action']}")
                for loser in losers:
                    await client.post(
                        f"{self._pl_url}/planning/plans/{winner['plan_id']}/append",
                        json={"action": loser["action"], "from_plan": loser["plan_id"]}
                    )
                    await client.delete(f"{self._pl_url}/planning/plans/{loser['plan_id']}")
            else:
                # PRIORITY: cancel losing plans, keep winner
                log.info(f"[Conflict] PRIORITY: keeping {winner['action']} (score={score(winner):.3f})")
                for loser in losers:
                    log.info(f"[Conflict] Cancelling plan {loser['plan_id']} for {loser['agent_id']}")
                    await client.delete(f"{self._pl_url}/planning/plans/{loser['plan_id']}")
                    await client.patch(
                        f"{self._wm_url}/world/tasks/{loser['task_id']}",
                        json={"status": "cancelled",
                              "reason": f"Conflict resolved in favour of {winner['agent_id']}"}
                    )
