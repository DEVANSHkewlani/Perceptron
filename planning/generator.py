"""
PlanGenerator — converts a DecisionObject dict into a concrete Plan.
Strategy priority: template_based → rule_based.
"""
from __future__ import annotations
import uuid
from .schema import Plan, PlanStep, SuccessCriterion, RollbackStep
from .template_store import PlanTemplateStore


class PlanGenerator:
    def __init__(
        self,
        template_store: PlanTemplateStore,
        llm_client=None,
    ):
        self.templates  = template_store
        self.llm_client = llm_client

    async def generate(self, decision: dict) -> Plan:
        """
        Generate a Plan from a DecisionObject dict.
        """
        action = decision.get("recommended_action", "")
        params = decision.get("action_parameters", {}) or {}
        dec_id = decision.get("decision_id", f"dec_{uuid.uuid4().hex[:10]}")

        # Strategy 1: template-based
        template = self.templates.get(action)
        if template:
            return self._from_template(template, action, params, dec_id, "template_based", decision)

        # Strategy 2: rule-based fallback (single-step plan)
        return self._single_step_plan(action, params, dec_id, decision)

    def _from_template(
        self, tmpl: dict, action: str, params: dict,
        dec_id: str, strategy: str, decision: dict
    ) -> Plan:
        """Render a plan_template.yaml entry with current parameters."""

        def render(s: str) -> str:
            if not isinstance(s, str):
                return s
            for k, v in params.items():
                s = s.replace(f"{{{k}}}", str(v))
            return s

        steps = []
        for raw_step in tmpl.get("steps", []):
            step_params = {**params, **raw_step.get("parameters", {})}
            
            rendered_params = {}
            for pk, pv in step_params.items():
                if isinstance(pv, str):
                    rendered_params[pk] = render(pv)
                else:
                    rendered_params[pk] = pv

            steps.append(PlanStep(
                step_id         = raw_step["id"],
                action          = raw_step.get("action", action),
                description     = render(raw_step.get("description", "")),
                parameters      = rendered_params,
                depends_on      = raw_step.get("depends_on", []),
                timeout_s       = raw_step.get("timeout_s", 60),
                on_failure      = raw_step.get("on_failure", "rollback"),
                is_approval_gate= raw_step.get("is_approval_gate", False),
            ))

        criteria = [
            SuccessCriterion(
                type=c["type"], check=render(c["check"]),
                timeout_s=c.get("timeout_s", 300),
            )
            for c in tmpl.get("success_criteria", [])
        ]
        
        rollback = [
            RollbackStep(
                action=r["action"],
                description=render(r.get("description", "")),
                parameters={k: render(str(v)) for k, v in r.get("parameters", {}).items()},
            )
            for r in tmpl.get("rollback", [])
        ]

        return Plan(
            decision_id      = dec_id,
            goal             = render(tmpl.get("goal", f"Execute {action}")),
            preconditions    = [render(p) for p in tmpl.get("preconditions", [])],
            steps            = steps,
            success_criteria = criteria,
            rollback_plan    = rollback,
            approval_gates   = tmpl.get("approval_gates", []),
            strategy         = strategy,
            agent_id         = decision.get("agent_id"),
        )

    def _single_step_plan(
        self, action: str, params: dict, dec_id: str, decision: dict
    ) -> Plan:
        """Fallback: create a minimal one-step plan for any action."""
        requires_approval = decision.get("requires_human_approval", False)
        steps = []
        gates = []

        if requires_approval:
            steps.append(PlanStep(
                step_id="s0", action="human_handoff",
                description=f"Confirm: {action}",
                parameters={"reason": decision.get("situation_assessment", ""),
                            "urgency": "within_hour"},
                timeout_s=1800, on_failure="abort", is_approval_gate=True,
            ))
            gates.append("s0")

        steps.append(PlanStep(
            step_id="s1", action=action,
            description=f"Execute {action}",
            parameters=params,
            depends_on=["s0"] if requires_approval else [],
            timeout_s=120, on_failure="rollback",
        ))
        
        steps.append(PlanStep(
            step_id="s2", action="send_alert",
            description=f"Notify: {action} executed",
            parameters={"severity": "medium", "message": f"Auto-executed: {action}"},
            depends_on=["s1"], timeout_s=15, on_failure="continue",
        ))

        return Plan(
            decision_id=dec_id,
            goal=f"Execute {action} successfully",
            steps=steps, approval_gates=gates, strategy="rule_based",
            success_criteria=[SuccessCriterion(
                type="event_received", check="action_completed", timeout_s=180
            )],
            agent_id=decision.get("agent_id"),
        )
