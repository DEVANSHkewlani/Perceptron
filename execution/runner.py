"""
PlanRunner — drives a Plan to completion.
Reads plan.ready_steps() → dispatches to handler → calls PlanMonitor.on_step_completed().
Supports parallel execution of independent steps (those with no shared dependency).
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from planning.schema import Plan, PlanStep, StepStatus, PlanStatus
from planning.store import PlanStore
from planning.monitor import PlanMonitor
from .action_registry import ActionRegistry


class PlanRunner:
    def __init__(
        self,
        plan_store: PlanStore,
        plan_monitor: PlanMonitor,
        action_registry: ActionRegistry,
    ):
        self.store    = plan_store
        self.monitor  = plan_monitor
        self.registry = action_registry

    async def execute(self, plan: Plan) -> Plan:
        """
        Drive a Plan to completion or failure.
        Steps without shared dependencies execute concurrently.
        Approval gates pause execution until human approval is recorded.
        """
        plan.status = PlanStatus.RUNNING
        await self.store.save(plan)

        while not plan.is_complete() and plan.status == PlanStatus.RUNNING:
            ready = plan.ready_steps()
            if not ready:
                # All ready steps exhausted — check if plan is stuck
                pending = [s for s in plan.steps if s.status == StepStatus.PENDING]
                if pending:
                    # Dependencies failed or plan is waiting for approval
                    break
                break

            # Execute all ready steps in parallel
            await asyncio.gather(*[self._run_step(plan, step) for step in ready])
            await self.store.save(plan)

        if plan.status == PlanStatus.RUNNING and plan.is_complete():
            plan.status       = PlanStatus.SUCCEEDED
            plan.completed_at = datetime.now(timezone.utc)
            await self.store.save(plan)

        return plan

    async def _run_step(self, plan: Plan, step: PlanStep) -> None:
        if step.is_approval_gate:
            step.status = StepStatus.WAITING
            await self.store.save(plan)
            plan.status = PlanStatus.AWAITING_APPROVAL
        else:
            step.status = StepStatus.RUNNING

        step.started_at = datetime.now(timezone.utc)

        handler = self.registry.get(step.action)
        if not handler:
            step.status = StepStatus.FAILED
            step.error  = f"No handler registered for action: {step.action}"
            await self.monitor.on_step_completed(plan, step)
            return

        try:
            # We inject the current plan_id into step parameters if the action is human_handoff/escalate
            step_params = {**step.parameters}
            if step.action in ("human_handoff", "escalate_to_human"):
                step_params["plan_id"] = plan.plan_id
                step_params["timeout_s"] = step.timeout_s

            result = await asyncio.wait_for(
                handler.run(
                    action=step.action,
                    parameters=step_params,
                    plan_id=plan.plan_id,
                    step_id=step.step_id,
                    entity_refs=list(step.parameters.values())[:2],
                ),
                timeout=step.timeout_s,
            )
            step.status       = StepStatus.SUCCEEDED if result.success else StepStatus.FAILED
            step.result       = result.output
            step.error        = result.error
        except asyncio.TimeoutError:
            step.status = StepStatus.FAILED
            step.error  = f"Step timed out after {step.timeout_s}s"
        except Exception as e:
            step.status = StepStatus.FAILED
            step.error  = str(e)

        step.completed_at = datetime.now(timezone.utc)
        
        # Reset plan status to RUNNING if the approval gate is completed successfully
        if step.is_approval_gate and step.status == StepStatus.SUCCEEDED:
            plan.status = PlanStatus.RUNNING

        await self.monitor.on_step_completed(plan, step)
