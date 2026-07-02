"""
ReasoningEngine — orchestrates all eight steps.
Called by the trigger scheduler (every N seconds) or by direct API call.
Integrates: World Model → Fast Path → Memory → Prompt → LLM → Validate → Persist.
"""
from __future__ import annotations
import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import yaml

from .fast_path import RuleEngine
from .context_builder import ContextBuilder
from .prompt_builder import PromptBuilder
from .llm_client import LLMClient, LLMConfig
from .schema import DecisionObject
from .actions import ActionRegistry


@dataclass
class ReasoningConfig:
    world_model_url:   str = "http://localhost:8092"
    memory_api_url:    str = "http://localhost:8090"
    actions_yaml_path: str = "actions.yaml"
    llm_model:         str = "claude-3-5-sonnet-20241022"
    min_severity_to_reason: str = "medium"  # skip info-only situations
    run_interval_s:    int = 60


class ReasoningEngine:
    def __init__(self, cfg: ReasoningConfig | None = None, agent_id: str = "agent:reasoning-engine", domain: str = "general"):
        self.cfg        = cfg or ReasoningConfig()
        self.agent_id   = agent_id
        self.domain     = domain
        self.rule_engine= RuleEngine()
        self.ctx_builder= ContextBuilder(self.cfg.memory_api_url)
        self.llm_client = LLMClient(LLMConfig(model=self.cfg.llm_model))
        self._actions   = self._load_actions()
        self._action_reg = ActionRegistry(self._actions)
        self.prompt_bld = PromptBuilder(self._actions)
        self._http      = httpx.AsyncClient(timeout=15.0)

    def _load_actions(self) -> dict:
        try:
            with open(self.cfg.actions_yaml_path, "r") as f:
                data = yaml.safe_load(f) or {}
                return data.get("actions", {})
        except Exception as e:
            print(f"[ReasoningEngine] Failed to load actions.yaml: {e}")
            return {}

    async def _load_success_rates(self) -> dict[str, float]:
        """Load historical success rates from procedural memory."""
        try:
            r = await self._http.get(f"{self.cfg.memory_api_url}/memory/procedural/playbooks")
            if r.status_code == 200:
                return {
                    pb["recommended_action"]: pb["success_rate"]
                    for pb in r.json()
                    if pb.get("recommended_action") and pb.get("success_rate") is not None
                }
        except Exception as e:
            print(f"[ReasoningEngine] Error loading success rates: {e}")
        return {}

    def _is_domain_relevant(self, event_type: str) -> bool:
        if self.domain == "general":
            return True
        domain_keywords = {
            "database": ["database", "connection", "query", "replication", "deadlock"],
            "queue":    ["consumer", "queue", "lag", "dead_letter"],
            "service":  ["service", "latency", "health", "cpu", "memory"],
            "security": ["brute_force", "ddos", "injection", "waf"],
        }
        keywords = domain_keywords.get(self.domain, [])
        return any(kw in event_type.lower() for kw in keywords)

    async def reason(self) -> DecisionObject | None:
        """
        Orchestrate the 8-step reasoning process.
        Returns a DecisionObject if action is recommended, or None
        if no reasoning was required (no significant anomalies).
        """
        t0 = time.monotonic()

        # Step ① — Fetch situation from World Model
        situation = await self._fetch_situation()
        if not situation:
            return None

        # Filter anomalies to this agent's domain
        if self.domain != "general":
            situation["ranked_anomalies"] = [
                a for a in situation.get("ranked_anomalies", [])
                if self._is_domain_relevant(a.get("event_type", ""))
            ]
            # Recompute severity counts based on filtered anomalies
            severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "info": 0}
            for a in situation["ranked_anomalies"]:
                sev = a.get("severity")
                if sev in severity_counts:
                    severity_counts[sev] += 1
            if "system_health" in situation:
                situation["system_health"]["by_severity"] = severity_counts

        # Skip if no significant anomalies
        severity_counts = situation.get("system_health", {}).get("by_severity", {})
        if not any(
            severity_counts.get(s, 0) > 0
            for s in ("critical", "high", "medium")
        ):
            return None

        # Step ② — Fast path check with dynamic success rates
        rates = await self._load_success_rates()
        self.rule_engine = RuleEngine(rates)
        fast_decision = self.rule_engine.evaluate(situation)
        if fast_decision:
            await self._persist_decision(fast_decision, situation, fast_path=True)
            return fast_decision

        # Steps ③ + ④ — Fetch context (parallel)
        top_anomaly = (situation.get("ranked_anomalies") or [{}])[0]
        experiences, playbooks = await asyncio.gather(
            self.ctx_builder.get_past_experiences(situation),
            self.ctx_builder.get_activated_playbooks(
                top_anomaly.get("event_type", ""),
                top_anomaly.get("severity", "medium"),
            ),
        )

        # Step ⑤ — Build prompt
        exp_text = self.ctx_builder.format_experiences(experiences)
        prompt   = self.prompt_bld.build(situation, exp_text, playbooks, agent_id=self.agent_id, domain=self.domain)

        # Step ⑥ + ⑦ — LLM call + parse + validate
        decision = await self.llm_client.call_with_schema_correction(prompt)

        # Validate action exists in registry
        if not self._action_reg.is_valid(decision.recommended_action):
            decision.recommended_action = "escalate_to_human"
            decision.action_parameters  = {
                "reason": f"LLM recommended unknown action. Escalating.",
                "urgency": "within_hour",
            }
            decision.requires_human_approval = True

        # Auto-require approval if action metadata demands it
        action_meta = self._actions.get(decision.recommended_action, {})
        if action_meta.get("requires_approval"):
            decision.requires_human_approval = True

        # Step ⑧ — Persist to episodic memory
        await self._persist_decision(decision, situation, fast_path=False)

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        print(f"[ReasoningEngine] Decision in {elapsed_ms}ms | "
              f"action={decision.recommended_action} | "
              f"confidence={decision.confidence:.2f} | "
              f"approval={decision.requires_human_approval}")

        return decision

    async def run_loop(self):
        """Continuous reasoning loop — runs every cfg.run_interval_s seconds."""
        print(f"[ReasoningEngine] Starting loop (interval={self.cfg.run_interval_s}s)")
        while True:
            try:
                decision = await self.reason()
                if decision:
                    print(f"[ReasoningEngine] → {decision.recommended_action}")
            except Exception as e:
                print(f"[ReasoningEngine] Error in reasoning cycle: {e}")
            await asyncio.sleep(self.cfg.run_interval_s)

    # ── HELPERS ────────────────────────────────────────────────

    async def _fetch_situation(self) -> dict | None:
        try:
            r = await self._http.get(
                f"{self.cfg.world_model_url}/world/situation",
                params={"top_n": 5},
            )
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            import traceback
            print(f"[ReasoningEngine] World Model unreachable: {type(e)} {e}")
            traceback.print_exc()
        return None

    async def _persist_decision(
        self, decision: DecisionObject, situation: dict, fast_path: bool
    ) -> None:
        record = decision.to_episodic_record(situation)
        record["fast_path"]  = fast_path
        record["created_at"] = datetime.now(timezone.utc).isoformat()
        record["agent_id"]   = self.agent_id
        try:
            await self._http.post(
                f"{self.cfg.memory_api_url}/memory/episodic",
                json=record,
            )
        except Exception:
            pass   # non-fatal — decision is returned regardless
