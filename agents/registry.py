from __future__ import annotations


class AgentRegistry:
    def __init__(self):
        self._agents = {}

    def register(self, agent_id: str, agent_type: str, domain: str, metadata: dict | None = None) -> None:
        self._agents[agent_id] = {
            "type": agent_type,
            "domain": domain,
            "status": "running",
            "metadata": metadata or {}
        }

    def update_status(self, agent_id: str, status: str) -> None:
        if agent_id in self._agents:
            self._agents[agent_id]["status"] = status

    def get(self, agent_id: str) -> dict | None:
        return self._agents.get(agent_id)

    def list_all(self) -> dict[str, dict]:
        return self._agents.copy()
