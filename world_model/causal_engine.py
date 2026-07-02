"""
CausalEngine — Neo4j Cypher traversal for causal reasoning.
Methods:
  get_blast_radius(entity_id)     → who depends on this entity
  get_causal_chain(entity_id)     → what does this entity depend on (root cause path)
  get_related_anomalies(entity_id)→ graph-neighbours with active anomalies
  get_correlation_partners(entity_id) → HISTORICALLY_CORRELATED entities
"""
from __future__ import annotations
from dataclasses import dataclass
from neo4j import AsyncGraphDatabase


@dataclass
class BlastRadiusResult:
    affected_entity_id: str
    entity_type:        str
    relationship_type:  str
    hop_distance:       int


@dataclass
class CausalLink:
    from_entity:       str
    to_entity:         str
    relationship_type: str
    confidence:        float = 1.0


class CausalEngine:
    def __init__(self, uri: str, user: str, password: str):
        self._uri  = uri
        self._user = user
        self._pass = password
        self._driver = None

    async def connect(self):
        self._driver = AsyncGraphDatabase.driver(
            self._uri, auth=(self._user, self._pass)
        )

    async def disconnect(self):
        if self._driver: await self._driver.close()

    async def get_blast_radius(
        self, entity_id: str, max_hops: int = 3
    ) -> list[BlastRadiusResult]:
        """
        Who depends on this entity?
        Traverses DEPENDS_ON edges in reverse direction.
        If entity_id goes down, all returned entities are at risk.
        """
        async with self._driver.session() as session:
            result = await session.run(f"""
                MATCH path = (dependent:Entity)-[:DEPENDS_ON*1..{max_hops}]->(target:Entity {{id: $id}})
                RETURN DISTINCT
                    dependent.id          AS affected_id,
                    dependent.entity_type AS entity_type,
                    length(path)          AS hop_distance,
                    type(relationships(path)[0]) AS rel_type
                ORDER BY hop_distance ASC
            """, id=entity_id)
            rows = await result.data()

        # Also get COMMUNICATES_WITH peers (1 hop only)
        async with self._driver.session() as session:
            result2 = await session.run("""
                MATCH (peer:Entity)-[:COMMUNICATES_WITH]->(target:Entity {id: $id})
                RETURN DISTINCT
                    peer.id          AS affected_id,
                    peer.entity_type AS entity_type,
                    1                AS hop_distance,
                    'COMMUNICATES_WITH' AS rel_type
            """, id=entity_id)
            rows += await result2.data()

        return [
            BlastRadiusResult(
                affected_entity_id=r["affected_id"],
                entity_type=r["entity_type"] or "unknown",
                relationship_type=r["rel_type"] or "DEPENDS_ON",
                hop_distance=r["hop_distance"],
            )
            for r in rows
        ]

    async def get_causal_chain(
        self, entity_id: str, max_hops: int = 4
    ) -> list[CausalLink]:
        """
        What does this entity depend on (outward traversal)?
        Used to build root cause hypothesis.
        Returns the dependency path from entity → root dependency.
        """
        async with self._driver.session() as session:
            result = await session.run(f"""
                MATCH path = (src:Entity {{id: $id}})-[:DEPENDS_ON*1..{max_hops}]->(dep:Entity)
                WITH src, dep, path,
                     [r IN relationships(path) | type(r)] AS rel_types,
                     [n IN nodes(path) | n.id] AS node_ids,
                     length(path) AS path_len
                RETURN DISTINCT
                    node_ids[0]   AS from_entity,
                    node_ids[-1]  AS to_entity,
                    rel_types[0]  AS rel_type,
                    dep.confidence AS confidence,
                    path_len
                ORDER BY path_len ASC
            """, id=entity_id)
            rows = await result.data()

        return [
            CausalLink(
                from_entity=r["from_entity"],
                to_entity=r["to_entity"],
                relationship_type=r["rel_type"] or "DEPENDS_ON",
                confidence=float(r["confidence"]) if r["confidence"] is not None else 1.0,
            )
            for r in rows
        ]

    async def get_correlation_partners(
        self, entity_id: str
    ) -> list[dict]:
        """
        Entities with HISTORICALLY_CORRELATED edges to this one.
        Written by GraphTemporalBridge (Phase 5).
        """
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (a:Entity {id: $id})-[r:HISTORICALLY_CORRELATED]->(b:Entity)
                RETURN b.id AS partner_id, r.pearson_r AS r_value,
                       r.confidence AS confidence, r.event_type_b AS event_type
                ORDER BY r.pearson_r DESC
            """, id=entity_id)
            return await result.data()

    async def create_task_node(self, task: dict) -> None:
        """Create a Task node in Neo4j and link it to the target entity."""
        task_id = task["id"]
        action = task["action"]
        agent_id = task["agent_id"]
        # Find target entity from parameters
        entity_id = task["parameters"].get("entity_id")
        if not entity_id:
            # Fallback to general entity refs
            entity_id = task.get("entity_refs", ["general"])[0]

        async with self._driver.session() as session:
            await session.run("""
                MERGE (t:Task {id: $id})
                SET t.action = $action, t.status = 'pending', t.agent_id = $agent_id
                WITH t
                MERGE (e:Entity {id: $entity_id})
                MERGE (t)-[:TARGETS]->(e)
            """, id=task_id, action=action, agent_id=agent_id, entity_id=entity_id)

    async def update_task_node(self, task_id: str, status: str) -> None:
        """Update a Task node's status in Neo4j."""
        async with self._driver.session() as session:
            await session.run("""
                MATCH (t:Task {id: $id})
                SET t.status = $status
            """, id=task_id, status=status)

    async def detect_all_conflicts(self) -> list[dict]:
        """Find entities with more than 1 active task pointing to them."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (t:Task)-[:TARGETS]->(e:Entity)
                WHERE t.status = 'pending'
                WITH e, collect(t) as tasks
                WHERE size(tasks) > 1
                RETURN e.id AS entity_id,
                       [t IN tasks | {
                           task_id: t.id,
                           agent_id: t.agent_id,
                           action: t.action,
                           plan_id: coalesce(t.plan_id, 'plan_unknown'),
                           risk: coalesce(t.risk, 'medium'),
                           confidence: coalesce(t.confidence, 1.0)
                       }] AS tasks
            """)
            return await result.data()
