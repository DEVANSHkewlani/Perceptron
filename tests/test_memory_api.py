import pytest
import time
from fastapi.testclient import TestClient
from memory.api import app, vector

def test_memory_api_endpoints():
    import uuid
    # Override collection name to prevent live database pollution from failing the test
    vector.collection_name = "test_cognitive_events"
    
    unique_plan_id = f"test_plan_{uuid.uuid4().hex[:8]}"
    unique_action = f"escalate_to_human_{uuid.uuid4().hex[:8]}"
    unique_reason = f"Testing HTTP endpoints {uuid.uuid4().hex[:8]}"
    
    # TestClient inside context manager to run app lifespan (startup/shutdown)
    with TestClient(app) as client:
        # Wait for the background SentenceTransformer model to finish loading
        for _ in range(60):
            if vector._model is not None:
                break
            time.sleep(1)
        assert vector._model is not None, "SentenceTransformer model failed to load in background task during test"

        # Check health
        health_resp = client.get("/health")
        assert health_resp.status_code == 200
        assert health_resp.json() == {"status": "ok"}
        
        # 1. Post a reasoning completed decision
        record = {
            "situation_summary": {
                "situation_summary": "Test memory api anomaly situation.",
                "ranked_anomalies": [
                    {
                        "event_type": "test_api_anomaly",
                        "severity": "high",
                        "source_id": "svc:test-service",
                        "entity_id": "svc:test-service"
                    }
                ]
            },
            "decision": {
                "situation_assessment": "Assessing API test",
                "root_cause_hypothesis": {
                    "hypothesis": "Test hypothesis description",
                    "confidence": 0.85,
                    "evidence": ["evidence 1"]
                },
                "recommended_action": unique_action,
                "action_parameters": {"reason": unique_reason, "urgency": "high"},
                "confidence": 0.85,
                "requires_human_approval": True,
                "alternative_actions": [],
                "reasoning_trace": "Detailed trace of testing memory api"
            },
            "plan_id": unique_plan_id,
            "outcome": None
        }
        
        post_resp = client.post("/memory/episodic", json=record)
        assert post_resp.status_code == 200
        res_data = post_resp.json()
        assert res_data["status"] == "success"
        assert "event_id" in res_data
        
        # Wait slightly for Qdrant to index the vector point
        time.sleep(0.5)
        
        # 2. Search for the event using 'q'
        search_resp = client.get("/memory/episodic/search", params={"q": unique_reason})
        assert search_resp.status_code == 200
        search_data = search_resp.json()
        assert "results" in search_data
        
        results = search_data["results"]
        # Find our reasoning completed event and verify outcome is initially None
        found = False
        for exp in results:
            if exp.get("event_type") == "reasoning_completed" and exp.get("plan_id") == unique_plan_id:
                decision = exp.get("decision", {})
                if decision.get("recommended_action") == unique_action:
                    assert exp.get("outcome") is None
                    found = True
                    break
        assert found, "Could not find reasoning_completed event in episodic search"

        # 3. Patch the episodic record with success outcome
        patch_resp = client.patch(f"/memory/episodic/by-plan/{unique_plan_id}", json={
            "outcome": "success",
            "verified_at": "2024-01-15T14:24:00Z",
            "anomalies_before": 3,
            "anomalies_after": 0,
        })
        assert patch_resp.status_code == 200
        assert patch_resp.json()["rows"] == 1

        # Wait slightly for Qdrant update to propagate
        time.sleep(0.5)

        # 4. Search again and verify that Qdrant payload now has "success" outcome
        search_resp = client.get("/memory/episodic/search", params={"q": unique_reason})
        assert search_resp.status_code == 200
        results = search_resp.json()["results"]
        found_updated = False
        for exp in results:
            if exp.get("event_type") == "reasoning_completed" and exp.get("plan_id") == unique_plan_id:
                assert exp.get("outcome") == "success", f"Outcome in Qdrant was not updated: {exp.get('outcome')}"
                found_updated = True
                break
        assert found_updated, "Could not find patched reasoning_completed event in episodic search"
