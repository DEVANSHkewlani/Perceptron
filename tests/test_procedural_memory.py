import pytest
from datetime import datetime, timezone
from memory.procedural import ProceduralMemory

DSN = "postgresql://postgres:postgres@localhost:5432/cognitive"

SAMPLE_PLAYBOOK = {
    "id": "pb_test_001",
    "name": "Test Playbook",
    "trigger_event": "cpu_spike",
    "trigger_severity": "high",
    "steps": [
        {"step": 1, "action": "alert", "params": {"message": "High CPU!"}}
    ],
    "confidence": 0.90
}

@pytest.mark.asyncio
async def test_procedural_store_and_retrieve():
    pm = ProceduralMemory(DSN)
    await pm.connect()
    
    # Create playbook
    await pm.create_playbook(SAMPLE_PLAYBOOK)
    
    # Retrieve specific playbook
    pb = await pm.get_playbook("pb_test_001")
    assert pb is not None
    assert pb["name"] == "Test Playbook"
    assert pb["trigger_event"] == "cpu_spike"
    assert pb["steps"] == SAMPLE_PLAYBOOK["steps"]
    
    # Retrieve all playbooks
    all_pbs = await pm.get_all_playbooks()
    ids = [p["id"] for p in all_pbs]
    assert "pb_test_001" in ids
    
    await pm.disconnect()

@pytest.mark.asyncio
async def test_playbook_activation_trigger():
    pm = ProceduralMemory(DSN)
    await pm.connect()
    
    await pm.create_playbook(SAMPLE_PLAYBOOK)
    
    # Event matching both type and severity
    matching_event = {
        "event_id": "evt_proc_test_001",
        "event_type": "cpu_spike",
        "severity": "high",
        "timestamp": datetime.now(timezone.utc)
    }
    
    triggered = await pm.check_and_activate_playbooks(matching_event)
    assert len(triggered) == 1
    assert triggered[0]["id"] == "pb_test_001"
    
    # Verify last_used_at is updated
    updated_pb = await pm.get_playbook("pb_test_001")
    assert updated_pb["last_used_at"] is not None
    
    # Non-matching event (different type)
    non_matching_event = {
        "event_id": "evt_proc_test_002",
        "event_type": "disk_full",
        "severity": "high",
        "timestamp": datetime.now(timezone.utc)
    }
    
    triggered = await pm.check_and_activate_playbooks(non_matching_event)
    assert len(triggered) == 0
    
    # Non-matching event (different severity)
    diff_severity_event = {
        "event_id": "evt_proc_test_003",
        "event_type": "cpu_spike",
        "severity": "info",
        "timestamp": datetime.now(timezone.utc)
    }
    
    triggered = await pm.check_and_activate_playbooks(diff_severity_event)
    assert len(triggered) == 0
    
    await pm.disconnect()
