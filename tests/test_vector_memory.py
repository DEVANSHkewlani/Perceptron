import pytest
import asyncio
from memory.vector import VectorMemory

QDRANT_URL = "http://localhost:6333"

SAMPLE_EVENT = {
    "event_id": "evt_vec_test_001",
    "event_type": "database_connection_timeout",
    "severity": "high",
    "source_id": "svc:auth-service",
    "source_type": "log",
    "confidence": 0.95,
    "timestamp": "2024-01-15T14:23:11Z",
    "payload": {"database": "postgres-primary", "error": "connection timeout"},
    "entity_refs": ["svc:auth-service", "db:postgres-primary"],
    "tags": ["database", "timeout"]
}

SIMILAR_EVENT = {
    "event_id": "evt_vec_test_002",
    "event_type": "database_connection_error",
    "severity": "critical",
    "source_id": "svc:auth-service",
    "source_type": "log",
    "confidence": 0.98,
    "timestamp": "2024-01-15T14:24:00Z",
    "payload": {"database": "postgres-primary", "error": "pool connection failure"},
    "entity_refs": ["svc:auth-service", "db:postgres-primary"],
    "tags": ["database", "error"]
}

@pytest.mark.asyncio
async def test_vector_store_and_search():
    import uuid
    vm = VectorMemory(QDRANT_URL, collection_name="test_cognitive_events")
    await vm.connect()
    
    # Wait for the background SentenceTransformer model to finish loading
    for _ in range(60):
        if vm._model is not None:
            break
        await asyncio.sleep(1)
    assert vm._model is not None, "SentenceTransformer model failed to load in background task during test"

    unique_evt_1 = f"evt_vec_test_1_{uuid.uuid4().hex[:8]}"
    unique_evt_2 = f"evt_vec_test_2_{uuid.uuid4().hex[:8]}"

    event1 = {**SAMPLE_EVENT, "event_id": unique_evt_1}
    event2 = {**SIMILAR_EVENT, "event_id": unique_evt_2}
    
    # Store event
    await vm.store(event1)
    await vm.store(event2)
    
    # Wait a bit for Qdrant ingestion (usually instant but safe to let async loops finish)
    await asyncio.sleep(0.5)
    
    # Search by semantic query
    results = await vm.search("database connection timed out or failed to connect", limit=5)
    assert len(results) > 0
    event_ids = [r["event_id"] for r in results]
    assert unique_evt_1 in event_ids or unique_evt_2 in event_ids
    
    # Search similar events to a given event_id
    similar = await vm.search_similar_events(unique_evt_1, limit=5)
    assert len(similar) > 0
    similar_ids = [s["event_id"] for s in similar]
    # unique_evt_1 itself should be excluded
    assert unique_evt_1 not in similar_ids
    assert unique_evt_2 in similar_ids
    
    await vm.disconnect()
