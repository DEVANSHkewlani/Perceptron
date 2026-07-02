import time
import pytest
from memory.episodic import EpisodicMemory

DSN = "postgresql://postgres:postgres@localhost:5432/cognitive"

def get_fresh_timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

SAMPLE = {
    "event_id":    "evt_ep_test001",
    "event_type":  "cpu_spike",
    "severity":    "high",
    "source_id":   "metric:node-01",
    "source_type": "metric",
    "confidence":  0.98,
    "timestamp":   get_fresh_timestamp(),
    "ingested_at": get_fresh_timestamp(),
    "payload":     {"cpu_percent": 87.3},
    "entity_refs": ["metric:node-01"],
    "tags":        ["metric", "cpu"],
}

import uuid

@pytest.mark.asyncio
async def test_store_and_query_by_entity():
    em = EpisodicMemory(DSN)
    await em.connect()
    
    unique_id = f"evt_ep_{uuid.uuid4().hex}"
    test_event = {**SAMPLE, "event_id": unique_id}
    await em.store(test_event)

    rows = await em.query_by_entity("metric:node-01", hours=48)
    ids = [r["event_id"] for r in rows]
    assert unique_id in ids
    await em.disconnect()

@pytest.mark.asyncio
async def test_idempotent_insert():
    """Inserting same event twice should not raise."""
    em = EpisodicMemory(DSN)
    await em.connect()
    
    unique_id = f"evt_ep_{uuid.uuid4().hex}"
    test_event = {**SAMPLE, "event_id": unique_id}
    await em.store(test_event)
    await em.store(test_event)  # ON CONFLICT DO NOTHING
    
    rows = await em.query_by_entity("metric:node-01", hours=48)
    count = sum(1 for r in rows if r["event_id"] == unique_id)
    assert count == 1  # exactly one, not two
    await em.disconnect()

@pytest.mark.asyncio
async def test_query_by_severity():
    em = EpisodicMemory(DSN)
    await em.connect()
    
    unique_id = f"evt_ep_{uuid.uuid4().hex}"
    test_event = {**SAMPLE, "event_id": unique_id}
    await em.store(test_event)
    
    rows = await em.query_by_severity("high", hours=48)
    assert len(rows) > 0
    assert any(r["event_id"] == unique_id for r in rows)
    assert all(r["severity"] == "high" for r in rows)
    await em.disconnect()
