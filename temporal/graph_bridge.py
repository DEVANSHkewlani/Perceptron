"""
GraphTemporalBridge
Converts detected temporal patterns into Neo4j relationship updates.
- correlation pattern    → HISTORICALLY_CORRELATED edge
- recurrence pattern     → Concept node (known_recurring_event)
- spike/drift/absence    → updates entity status properties
"""
from __future__ import annotations

from datetime import datetime, timezone
from neo4j import AsyncGraphDatabase


class GraphTemporalBridge:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_pass: str):
        self._uri  = neo4j_uri
        self._user = neo4j_user
        self._pass = neo4j_pass
        self._driver = None

    async def connect(self):
        self._driver = AsyncGraphDatabase.driver(
            self._uri, auth=(self._user, self._pass)
        )

    async def disconnect(self):
        if self._driver: await self._driver.close()

    async def apply_pattern(self, pattern: dict) -> None:
        """Route pattern to the correct graph update method."""
        ptype = pattern.get("pattern_type")
        if   ptype == "correlation": await self._write_correlation(pattern)
        elif ptype == "recurrence":   await self._write_recurrence(pattern)
        elif ptype in ("spike", "drift", "absence"):
            await self._update_entity_status(pattern)

    async def _write_correlation(self, pattern: dict) -> None:
        """
        MERGE a HISTORICALLY_CORRELATED edge between two entities.
        Use MERGE so repeated detections strengthen (update) the edge
        rather than creating duplicates.
        """
        details = pattern.get("details", {})
        async with self._driver.session() as session:
            await session.run("""
                MERGE (a:Entity {id: $entity_a})
                MERGE (b:Entity {id: $entity_b})
                MERGE (a)-[r:HISTORICALLY_CORRELATED]->(b)
                ON CREATE SET
                    r.first_seen    = $now,
                    r.pearson_r     = $r_value,
                    r.sample_count  = $samples,
                    r.event_type_a  = $et_a,
                    r.event_type_b  = $et_b,
                    r.confidence    = $confidence
                ON MATCH SET
                    r.last_seen     = $now,
                    r.pearson_r     = $r_value,
                    r.sample_count  = $samples,
                    r.confidence    = $confidence,
                    r.update_count  = coalesce(r.update_count, 0) + 1
            """,
            entity_a   = pattern["entity_id"],
            entity_b   = pattern["entity_id_b"],
            now        = datetime.now(timezone.utc).isoformat(),
            r_value    = details.get("pearson_r"),
            samples    = details.get("sample_count"),
            et_a       = details.get("event_type_a"),
            et_b       = details.get("event_type_b"),
            confidence = pattern.get("confidence"),
        )

    async def _write_recurrence(self, pattern: dict) -> None:
        """
        Create a Concept node for the recurring pattern and link it to the entity.
        """
        details = pattern.get("details", {})
        concept_id = (
            f"concept:recurring:{pattern['entity_id']}:"
            f"{details.get('event_type')}:{details.get('hour_of_week')}"
        )
        async with self._driver.session() as session:
            await session.run("""
                MERGE (e:Entity {id: $entity_id})
                MERGE (c:Concept {id: $concept_id})
                ON CREATE SET
                    c.name        = 'known_recurring_event',
                    c.event_type  = $event_type,
                    c.hour_of_week= $hour_of_week,
                    c.confidence  = $confidence,
                    c.first_seen  = $now
                ON MATCH SET
                    c.confidence  = $confidence,
                    c.last_seen   = $now,
                    c.past_count  = $past_count
                MERGE (e)-[:TRIGGERS]->(c)
            """,
            entity_id   = pattern["entity_id"],
            concept_id  = concept_id,
            event_type  = details.get("event_type"),
            hour_of_week= details.get("hour_of_week"),
            confidence  = pattern.get("confidence"),
            now         = datetime.now(timezone.utc).isoformat(),
            past_count  = details.get("past_count", 0),
        )

    async def _update_entity_status(self, pattern: dict) -> None:
        """Update entity node properties with latest anomaly state."""
        async with self._driver.session() as session:
            await session.run("""
                MERGE (e:Entity {id: $entity_id})
                SET e.last_anomaly_type = $pattern_type,
                    e.last_anomaly_at   = $now,
                    e.anomaly_severity  = $severity
            """,
            entity_id    = pattern["entity_id"],
            pattern_type = pattern["pattern_type"],
            now          = datetime.now(timezone.utc).isoformat(),
            severity     = pattern["severity"],
        )
