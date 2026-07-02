"""
ApiCallHandler — executes REST API actions with idempotency and retry.
Used for: scale_consumer_group, scale_read_replicas, enable_circuit_breaker, etc.
"""
from __future__ import annotations
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from .base import BaseActionHandler

# Maps action name → (method, URL template, body builder)
API_ACTION_MAP: dict = {
    "scale_consumer_group": {
        "method": "POST",
        "url": "http://kafka-admin:8080/v3/clusters/{cluster}/consumer-groups/{consumer_group}/scale",
        "body": lambda p: {"delta": p.get("instance_delta", 2)},
    },
    "scale_read_replicas": {
        "method": "PATCH",
        "url": "http://postgres-operator/api/v1/clusters/{db_id}/replicas",
        "body": lambda p: {"replica_count": p.get("replica_count")},
    },
    "restart_connection_pool": {
        "method": "POST",
        "url": "http://pgbouncer-mgmt/pools/{service_id}/restart",
        "body": lambda p: {},
    },
    "scale_service_horizontal": {
        "method": "POST",
        "url": "http://k8s-operator/apis/apps/v1/namespaces/{namespace}/deployments/{deployment_id}/scale",
        "body": lambda p: {"replicas": p.get("replica_count", 3)},
    },
    "enable_circuit_breaker": {
        "method": "POST",
        "url": "http://istio-mgmt/rules/circuit-breaker/{service_id}",
        "body": lambda p: {
            "consecutive_gateway_errors": p.get("errors", 5),
            "base_ejection_time_s": p.get("duration_s", 30)
        },
    },
    "restart_service": {
        "method": "POST",
        "url": "http://k8s-operator/apis/apps/v1/namespaces/default/deployments/{service_id}/restart",
        "body": lambda p: {},
    },
    "increase_log_verbosity": {
        "method": "PUT",
        "url": "http://{service_id}/mgmt/logging",
        "body": lambda p: {"level": "DEBUG"},
    },
}


class ApiCallHandler(BaseActionHandler):
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    async def execute(self, action: str, parameters: dict) -> dict:
        spec = API_ACTION_MAP.get(action)
        if not spec:
            raise ValueError(f"No API spec for action: {action}")
        url  = spec["url"].format(**parameters)
        body = spec["body"](parameters)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.request(spec["method"], url, json=body)
            resp.raise_for_status()
            return {"status_code": resp.status_code, "response": resp.text[:500]}
