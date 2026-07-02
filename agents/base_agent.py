from __future__ import annotations
import asyncio, json, logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
import httpx
from aiokafka import AIOKafkaProducer


@dataclass
class AgentConfig:
    agent_id:         str
    agent_type:       str
    kafka_bootstrap:  str   = "localhost:9092"
    world_model_url:  str   = "http://localhost:8092"
    memory_url:       str   = "http://localhost:8090"
    planning_url:     str   = "http://localhost:8094"
    execution_url:    str   = "http://localhost:8095"
    reasoning_url:    str   = "http://localhost:8093"
    domain:           str   = "general"       # e.g. "database", "queue", "service"
    action_vocab:     list  = field(default_factory=list)  # subset of actions.yaml
    heartbeat_s:      int   = 30


class BaseAgent(ABC):
    def __init__(self, cfg: AgentConfig):
        self.cfg      = cfg
        self.agent_id = cfg.agent_id
        self._log     = logging.getLogger(f"agent.{cfg.agent_id}")
        self._producer: AIOKafkaProducer | None = None
        self._http:    httpx.AsyncClient  | None = None
        self._running = False

    async def start(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=self.cfg.kafka_bootstrap)
        await self._producer.start()
        self._http    = httpx.AsyncClient(timeout=12.0)
        self._running = True
        self._log.info(f"[{self.agent_id}] Started (type={self.cfg.agent_type})")
        await self.emit_event("action_started", "info", {"status": "online"})

    async def stop(self):
        self._running = False
        await self.emit_event("action_completed", "info", {"status": "offline"})
        if self._producer: await self._producer.stop()
        if self._http:     await self._http.aclose()

    @abstractmethod
    async def run(self): ...

    async def emit_event(
        self, event_type: str, severity: str,
        payload: dict, entity_refs: list[str] | None = None,
    ) -> None:
        """
        Publish to cognitive.events. This is the ONLY way agents write state.
        Shared perception contract enforced here.
        """
        import uuid
        NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        timestamp_str = datetime.now(timezone.utc).isoformat()
        natural_key = f"{self.agent_id}:{event_type}:{timestamp_str}"
        det_uuid = uuid.uuid5(NAMESPACE, natural_key).hex[:12]
        event_id = f"evt_{det_uuid}"

        event = {
            "event_id":    event_id,
            "event_type":  event_type,
            "source_type": "agent_event",
            "source_id":   self.agent_id,
            "agent_id":    self.agent_id,
            "severity":    severity,
            "confidence":  1.0,
            "entity_refs": entity_refs or [self.agent_id],
            "payload":     payload,
            "timestamp":   timestamp_str,
            "ingested_at": timestamp_str,
            "tags":        ["agent", self.cfg.agent_type]
        }
        await self._producer.send_and_wait(
            "cognitive.events", json.dumps(event).encode()
        )

    async def get_situation(self, top_n: int = 3) -> dict:
        """Query World Model for current situation — filtered to agent's domain."""
        r = await self._http.get(
            f"{self.cfg.world_model_url}/world/situation",
            params={"top_n": top_n},
        )
        if r.status_code == 200:
            situation = r.json()
            if self.cfg.domain != "general" and self.cfg.action_vocab:
                situation["ranked_anomalies"] = [
                    a for a in situation.get("ranked_anomalies", [])
                    if self._is_domain_relevant(a)
                ]
            return situation
        return {}

    def _is_domain_relevant(self, anomaly: dict) -> bool:
        if self.cfg.domain == "general": return True
        if not self.cfg.action_vocab: return True
        event_type = anomaly.get("event_type", "")
        domain_keywords = {
            "database": ["database", "connection", "query", "replication", "deadlock"],
            "queue":    ["consumer", "queue", "lag", "dead_letter"],
            "service":  ["service", "latency", "health", "cpu", "memory"],
            "security": ["brute_force", "ddos", "injection", "waf"],
        }
        keywords = domain_keywords.get(self.cfg.domain, [])
        return any(kw in event_type.lower() for kw in keywords)

    async def heartbeat_loop(self):
        while self._running:
            await asyncio.sleep(self.cfg.heartbeat_s)
            await self.emit_event("action_completed", "info",
                                  {"heartbeat": True, "agent_type": self.cfg.agent_type})
