import httpx
import time
import random
from datetime import datetime, timezone

url = "http://localhost:8080/perception/prometheus-alerts"

print("Seeding 25 normal CPU events...")
for i in range(25):
    payload = {
        "alerts": [{
            "status": "firing",
            "labels": {
                "alertname": "HighCPUUsage",
                "job": "auth-service",
                "instance": "node-01:9090"
            },
            "annotations": {
                "summary": "CPU high",
                "value": str(random.randint(40, 50))
            },
            "startsAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }]
    }
    try:
        r = httpx.post(url, json=payload)
        print(f"Sent event {i+1}: {r.status_code} -> {r.json()}")
    except Exception as e:
        print(f"Failed to send event {i+1}: {e}")
    time.sleep(0.1)

print("\nSending CPU Spike event (98%)...")
spike_payload = {
    "alerts": [{
        "status": "firing",
        "labels": {
            "alertname": "HighCPUUsage",
            "job": "auth-service",
            "instance": "node-01:9090"
        },
        "annotations": {
            "value": "98"
        },
        "startsAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }]
}
try:
    r = httpx.post(url, json=spike_payload)
    print(f"Sent spike event: {r.status_code} -> {r.json()}")
except Exception as e:
    print(f"Failed to send spike event: {e}")
