import pytest
from memory.semantic import SemanticMemory

SAMPLE = {
    "event_id":    "evt_sem_test001",
    "event_type":  "database_connection_timeout",
    "severity":    "high",
    "source_id":   "svc:auth-service",
    "source_type": "log",
    "timestamp":   "2024-01-15T14:23:11Z",
    "entity_refs": ["svc:auth-service", "db:postgres-primary"],
}

@pytest.mark.asyncio
async def test_entity_nodes_created():
    sm = SemanticMemory("bolt://localhost:7687", "neo4j", "password123")
    await sm.connect()
    await sm.store(SAMPLE)

    count = await sm.get_entity_count()
    assert count >= 2  # at least svc: and db: nodes exist
    await sm.disconnect()

@pytest.mark.asyncio
async def test_neighbors_found():
    sm = SemanticMemory("bolt://localhost:7687", "neo4j", "password123")
    await sm.connect()
    await sm.store(SAMPLE)

    neighbors = await sm.get_neighbors("svc:auth-service")
    ids = [n["id"] for n in neighbors]
    assert "db:postgres-primary" in ids
    await sm.disconnect()

@pytest.mark.asyncio
async def test_blast_radius():
    sm = SemanticMemory("bolt://localhost:7687", "neo4j", "password123")
    await sm.connect()
    await sm.store(SAMPLE)

    blast = await sm.get_blast_radius("db:postgres-primary")
    ids = [b["id"] for b in blast]
    assert "svc:auth-service" in ids  # auth-service is in blast radius of db
    await sm.disconnect()
