import asyncio
import os
import time
from fastapi.testclient import TestClient
from memory.api import app

def test_endpoints():
    with TestClient(app) as client:
        health_resp = client.get("/health")
        print("Health:", health_resp.status_code, health_resp.json())
        
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
                "recommended_action": "escalate_to_human",
                "action_parameters": {"reason": "Testing HTTP endpoints", "urgency": "high"},
                "confidence": 0.85,
                "requires_human_approval": True,
                "alternative_actions": [],
                "reasoning_trace": "Detailed trace of testing memory api"
            },
            "outcome": None
        }
        
        post_resp = client.post("/memory/episodic", json=record)
        print("Post status:", post_resp.status_code)
        res_data = post_resp.json()
        print("Post response:", res_data)
        
        event_id = res_data.get("event_id")
        
        time.sleep(1.0) # Wait 1 second to be extra sure Qdrant has indexed
        
        search_resp = client.get("/memory/episodic/search", params={"q": "Testing HTTP endpoints"})
        print("Search status:", search_resp.status_code)
        search_data = search_resp.json()
        print("Search results:")
        for r in search_data.get("results", []):
            print("- event_type:", r.get("event_type"), "decision:", r.get("decision", {}).get("recommended_action") if isinstance(r, dict) else r)

if __name__ == "__main__":
    test_endpoints()
