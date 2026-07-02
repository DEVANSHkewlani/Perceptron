import pytest
import time
from memory.router import MemoryRouter
from memory.working import WorkingMemory
from memory.episodic import EpisodicMemory
from memory.semantic import SemanticMemory

CRITICAL_EVENT = {
    "event_id":    "evt_route_test_001",
    "event_type":  "connection_pool_exhausted",
    "severity":    "critical",
    "source_id":   "svc:auth-service",
    "source_type": "log",
    "confidence":  0.95,
    "timestamp":   "2024-01-15T14:23:11Z",
    "ingested_at": "2024-01-15T14:23:12Z",
    "payload":     {"pool_size": 20, "waiting": 47},
    "entity_refs": ["svc:auth-service", "db:postgres-primary"],
    "tags":        ["database", "critical"],
}

INFO_EVENT = {
    "event_id":    "evt_route_test_002",
    "event_type":  "service_health_restored",
    "severity":    "info",
    "source_id":   "svc:order-service",
    "source_type": "api",
    "confidence":  0.97,
    "timestamp":   "2024-01-15T14:25:00Z",
    "ingested_at": "2024-01-15T14:25:01Z",
    "payload":     {"latency_ms": 42},
    "entity_refs": ["svc:order-service"],
    "tags":        ["api"],
}

@pytest.mark.asyncio
async def test_critical_routes_to_all_three_layers():
    router = MemoryRouter()
    await router.start()
    
    # Generate unique event_id to bypass Redis deduplication filter
    event_id = f"evt_route_test_001_{int(time.time() * 1000)}"
    # We must ensure the event timestamp is fresh for WorkingMemory staleness check.
    # Set to current time to pass the audit
    fresh_event = {
        **CRITICAL_EVENT,
        "event_id": event_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    
    await router._route(fresh_event)  # call route directly, skip Kafka

    # 1. Check working memory (Redis)
    events = await router.working.get_recent_events(10)
    assert any(e["event_id"] == event_id for e in events), \
        "CRITICAL event not found in working memory"

    # 2. Check episodic memory (TimescaleDB)
    rows = await router.episodic.query_by_entity("svc:auth-service", hours=48)
    assert any(r["event_id"] == event_id for r in rows), \
        "CRITICAL event not found in episodic memory"

    # 3. Check semantic memory (Neo4j)
    neighbors = await router.semantic.get_neighbors("svc:auth-service")
    neighbor_ids = [n["id"] for n in neighbors]
    assert "db:postgres-primary" in neighbor_ids, \
        "db:postgres-primary not found as neighbor of svc:auth-service"

    await router.stop()

@pytest.mark.asyncio
async def test_info_skips_working_memory():
    """INFO events must NOT go to working memory."""
    router = MemoryRouter()
    await router.start()
    
    event_id = f"evt_route_test_002_{int(time.time() * 1000)}"
    fresh_event = {
        **INFO_EVENT,
        "event_id": event_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    await router._route(fresh_event)

    # Should be in episodic
    rows = await router.episodic.query_by_entity("svc:order-service", hours=48)
    assert any(r["event_id"] == event_id for r in rows)

    # Should NOT be in working memory
    w_events = await router.working.get_recent_events(100)
    assert not any(e["event_id"] == event_id for e in w_events), \
        "INFO event should NOT be in working memory"

    await router.stop()
