"""
GraphUpdater — writes RESOLVES edges to Neo4j.

Knowledge accumulated here:
  (Action {id:'action:scale_consumer_group'})
      -[:RESOLVES {confidence:0.91, success_count:23, failure_count:2}]->
  (Concept {id:'concept:consumer_lag_critical', type:'anomaly_type'})
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from neo4j import AsyncGraphDatabase

log = logging.getLogger("feedback.graph")


class GraphUpdater:
    def __init__(self, uri: str, user: str, password: str, config: dict):
        self._uri    = uri
        self._user   = user
        self._pass   = password
        cfg          = config.get("defaults", {})
        self._inc    = cfg.get("confidence_increment", 0.04)
        self._dec    = cfg.get("confidence_decrement", 0.05)
        self._min    = cfg.get("confidence_min",       0.10)
        self._max    = cfg.get("confidence_max",       0.99)
        self._driver = None

    async def connect(self):
        self._driver = AsyncGraphDatabase.driver(self._uri, auth=(self._user, self._pass))
        log.info("[GraphUpdater] Connected to Neo4j")

    async def disconnect(self):
        if self._driver:
            await self._driver.close()

    async def update(self, action: str, payload: dict, outcome: str) -> None:
        """Write or update RESOLVES edge. Success = strengthen. Failure = weaken."""
        # Infer anomaly type from payload (what triggered this plan)
        anomaly_type = (
            payload.get("anomaly_type")
            or payload.get("event_type")
            or "unknown_anomaly"
        )
        now     = datetime.now(timezone.utc).isoformat()
        success = outcome == "success"

        async with self._driver.session() as session:
            if success:
                await session.run("""
                    MERGE (a:Action {id: $action_id})
                      ON CREATE SET a.name = $action, a.created_at = $now
                    MERGE (c:Concept {id: $concept_id})
                      ON CREATE SET
                        c.name = $anomaly_type,
                        c.type = 'anomaly_type',
                        c.created_at = $now
                    MERGE (a)-[r:RESOLVES]->(c)
                    ON CREATE SET
                      r.confidence    = 0.60,
                      r.success_count = 1,
                      r.failure_count = 0,
                      r.first_seen    = $now,
                      r.last_seen     = $now
                    ON MATCH SET
                      r.confidence = CASE
                        WHEN r.confidence + $inc > $max THEN $max
                        ELSE r.confidence + $inc END,
                      r.success_count = r.success_count + 1,
                      r.last_seen     = $now
                """,
                action_id   = f"action:{action}",
                action      = action,
                concept_id  = f"concept:{anomaly_type}",
                anomaly_type= anomaly_type,
                now=now, inc=self._inc, max=self._max,
                )
                log.info(f"[GraphUpdater] Strengthened RESOLVES: {action} → {anomaly_type}")
            else:
                await session.run("""
                    MATCH (a:Action {id: $action_id})-[r:RESOLVES]->(c:Concept {id: $concept_id})
                    SET r.confidence = CASE
                          WHEN r.confidence - $dec < $min THEN $min
                          ELSE r.confidence - $dec END,
                        r.failure_count = coalesce(r.failure_count, 0) + 1,
                        r.last_seen     = $now
                """,
                action_id  = f"action:{action}",
                concept_id = f"concept:{anomaly_type}",
                now=now, dec=self._dec, min=self._min,
                )
                log.info(f"[GraphUpdater] Weakened RESOLVES: {action} → {anomaly_type}")
