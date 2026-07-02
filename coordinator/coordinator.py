from __future__ import annotations
import asyncio, logging
from datetime import datetime, timezone
import httpx
from agents.base_agent import AgentConfig
from agents.monitor_agent import MonitorAgent
from agents.planner_agent import PlannerAgent
from agents.executor_agent import ExecutorAgent
from agents.memory_agent import MemoryAgent
from agents.prediction_agent import PredictionAgent
from .conflict_resolver import ConflictResolver

log = logging.getLogger("coordinator")

WORLD_MODEL_URL = "http://localhost:8092"
KAFKA           = "localhost:9092"


def build_agent_fleet() -> list:
    base = {"kafka_bootstrap": KAFKA, "world_model_url": WORLD_MODEL_URL}
    all_actions = [
        "restart_connection_pool", "kill_slow_query", "scale_read_replicas",
        "restart_service", "scale_service_horizontal", "enable_circuit_breaker",
        "scale_consumer_group", "purge_dead_letter_queue", "send_alert",
        "escalate_to_human", "increase_log_verbosity", "monitor_and_wait"
    ]

    return [
        # One global monitor — watches everything
        MonitorAgent(AgentConfig(agent_id="agent:monitor-global",
            agent_type="monitor", domain="general", **base),
            poll_interval_s=15, trigger_severity="medium"),

        # Pool-balanced planners
        PlannerAgent(AgentConfig(agent_id="agent:planner-01",
            agent_type="planner", domain="general",
            action_vocab=all_actions, **base),
            executor_agent_id="agent:executor-01"),

        PlannerAgent(AgentConfig(agent_id="agent:planner-02",
            agent_type="planner", domain="general",
            action_vocab=all_actions, **base),
            executor_agent_id="agent:executor-02"),

        PlannerAgent(AgentConfig(agent_id="agent:planner-03",
            agent_type="planner", domain="general",
            action_vocab=all_actions, **base),
            executor_agent_id="agent:executor-03"),

        # Pool-balanced executors
        ExecutorAgent(AgentConfig(agent_id="agent:executor-01",
            agent_type="executor", domain="general", **base)),
        ExecutorAgent(AgentConfig(agent_id="agent:executor-02",
            agent_type="executor", domain="general", **base)),
        ExecutorAgent(AgentConfig(agent_id="agent:executor-03",
            agent_type="executor", domain="general", **base)),

        # Background agents
        MemoryAgent(AgentConfig(agent_id="agent:memory",
            agent_type="memory", domain="general", **base)),
        PredictionAgent(AgentConfig(agent_id="agent:prediction",
            agent_type="prediction", domain="general", **base)),
    ]


class AgentCoordinator:
    def __init__(self):
        self.agents    = build_agent_fleet()
        self.resolver  = ConflictResolver(WORLD_MODEL_URL)
        self._tasks:   list[asyncio.Task] = []
        self._http     = httpx.AsyncClient(timeout=8.0)
        self._registry: dict[str, dict]  = {}

    async def run(self):
        """Start all agents concurrently and run conflict detection loop."""
        log.info(f"[Coordinator] Starting {len(self.agents)} agents")
        for agent in self.agents:
            t = asyncio.create_task(
                self._run_agent_safe(agent),
                name=agent.agent_id
            )
            self._tasks.append(t)
            self._registry[agent.agent_id] = {
                "type":       agent.cfg.agent_type,
                "domain":     agent.cfg.domain,
                "status":     "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
            }

        try:
            await asyncio.gather(
                asyncio.gather(*self._tasks, return_exceptions=True),
                self._conflict_detection_loop(),
            )
        except asyncio.CancelledError:
            log.info("[Coordinator] Cancelled all agent runs")
            for t in self._tasks:
                t.cancel()
        finally:
            await self._http.aclose()

    async def _run_agent_safe(self, agent) -> None:
        try:
            await agent.run()
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            log.error(f"[Coordinator] Agent {agent.agent_id} crashed: {e}\n{tb}")
            self._registry[agent.agent_id]["status"] = "crashed"
            self._registry[agent.agent_id]["error"] = str(e)
            self._registry[agent.agent_id]["stack_trace"] = tb

    async def _conflict_detection_loop(self):
        """Every 20s, check Neo4j for agents with overlapping task targets."""
        while True:
            await asyncio.sleep(20)
            try:
                r = await self._http.get(
                    f"{WORLD_MODEL_URL}/world/conflicts"
                )
                if r.status_code == 200:
                    conflicts = r.json()
                    for conflict in conflicts:
                        asyncio.create_task(
                            self.resolver.resolve(conflict)
                        )
            except Exception as e:
                log.warning(f"[Coordinator] Conflict detection error: {e}")

    def get_registry(self) -> dict:
        return self._registry.copy()
