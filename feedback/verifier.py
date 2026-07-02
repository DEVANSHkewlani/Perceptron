"""
OutcomeVerifier — measures the effect of an action on the world.

Verification strategy:
  BEFORE: snapshot the World Model anomaly count at event receive time
  WAIT:   per-action configurable delay from feedback_config.yaml
  AFTER:  re-query World Model anomaly count

  fewer anomalies  → success
  same anomalies   → partial (action did not resolve the situation)
  more anomalies   → failure (action may have made things worse)
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import httpx


@dataclass
class VerificationResult:
    plan_id:           str
    action:            str
    outcome:           Literal["success", "partial", "failure"]
    anomalies_before:  int
    anomalies_after:   int
    entity_health_before: str | None = None
    entity_health_after:  str | None = None
    verified_at:       str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    delay_used_s:      int = 90
    notes:             str = ""


class OutcomeVerifier:
    def __init__(self, world_model_url: str, config: dict):
        self._wm_url = world_model_url
        self._config = config

    def _get_delay(self, action: str) -> int:
        overrides = self._config.get("action_overrides", {})
        defaults  = self._config.get("defaults", {})
        
        # If action overrides has the config under action key
        action_cfg = overrides.get(action, {})
        if isinstance(action_cfg, dict) and "verification_delay_s" in action_cfg:
            return action_cfg["verification_delay_s"]
        return defaults.get("verification_delay_s", 90)

    async def verify(
        self, plan_id: str, action: str, event: dict
    ) -> VerificationResult:
        delay = self._get_delay(action)
        entity_refs = event.get("payload", {}).get("entity_refs", [])
        primary_entity = entity_refs[0] if entity_refs else None

        # Snapshot BEFORE
        before_count  = await self._count_anomalies()
        before_health = await self._get_entity_health(primary_entity)

        # Wait for environment to propagate action effects
        await asyncio.sleep(delay)

        # Snapshot AFTER
        after_count  = await self._count_anomalies()
        after_health = await self._get_entity_health(primary_entity)

        # Determine outcome
        if after_count < before_count:
            outcome = "success"
        elif after_count == before_count:
            # Secondary signal: entity health improvement counts as partial success
            if before_health == "critical" and after_health in ("degraded", "healthy"):
                outcome = "partial"
            elif before_health == "degraded" and after_health == "healthy":
                outcome = "success"
            else:
                outcome = "partial"
        else:
            outcome = "failure"

        return VerificationResult(
            plan_id=plan_id,
            action=action,
            outcome=outcome,
            anomalies_before=before_count,
            anomalies_after=after_count,
            entity_health_before=before_health,
            entity_health_after=after_health,
            delay_used_s=delay,
            notes=(
                f"Anomalies: {before_count}→{after_count} | "
                f"Entity health: {before_health}→{after_health} | "
                f"Delay: {delay}s"
            ),
        )

    async def _count_anomalies(self) -> int:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{self._wm_url}/world/anomalies")
                if r.status_code == 200:
                    return len(r.json())
        except Exception:
            pass
        return 0

    async def _get_entity_health(self, entity_id: str | None) -> str | None:
        if not entity_id:
            return None
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{self._wm_url}/world/entity/{entity_id}")
                if r.status_code == 200:
                    return r.json().get("health_status")
        except Exception:
            pass
        return None
