import pytest
import asyncio
from memory.working import WorkingMemory

SAMPLE = {
    "event_id":    "evt_test001",
    "event_type":  "database_connection_timeout",
    "severity":    "high",
    "source_id":   "svc:auth-service",
    "source_type": "log",
    "confidence":  0.82,
    "timestamp":   "2024-01-15T14:23:11Z",
    "payload":     {"database": "postgres-primary"},
    "entity_refs": ["svc:auth-service", "db:postgres-primary"],
    "tags":        ["log", "database"],
}

@pytest.mark.asyncio
async def test_store_and_retrieve():
    wm = WorkingMemory()
    await wm.connect()
    await wm.store(SAMPLE)

    events = await wm.get_recent_events(10)
    ids = [e["event_id"] for e in events]
    assert "evt_test001" in ids, "Event not found in recent events"
    await wm.disconnect()

@pytest.mark.asyncio
async def test_entity_state_written():
    wm = WorkingMemory()
    await wm.connect()
    await wm.store(SAMPLE)

    state = await wm.get_entity_state("svc:auth-service")
    assert state is not None
    assert state["last_event"] == "database_connection_timeout"
    assert state["severity"] == "high"
    await wm.disconnect()

@pytest.mark.asyncio
async def test_critical_filter():
    wm = WorkingMemory()
    await wm.connect()
    critical_event = {**SAMPLE, "event_id": "evt_crit001", "severity": "critical"}
    await wm.store(critical_event)

    criticals = await wm.get_active_critical()
    ids = [e["event_id"] for e in criticals]
    assert "evt_crit001" in ids
    await wm.disconnect()
