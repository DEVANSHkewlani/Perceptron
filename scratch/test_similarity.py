import json
import numpy as np
from sentence_transformers import SentenceTransformer

def cosine_similarity(v1, v2):
    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

def test():
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    # Payload matching our record
    payload = {
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
    
    event_type = "reasoning_completed"
    source_id = "agent:reasoning-engine"
    
    # Build text exactly as stored
    text = f"Type: {event_type} | Source: {source_id} | Payload: {json.dumps(payload)}"
    
    emb_doc = model.encode(text)
    emb_query = model.encode("Testing HTTP endpoints")
    
    sim = cosine_similarity(emb_doc, emb_query)
    print("Cosine Similarity between doc and query:", sim)

if __name__ == "__main__":
    test()
