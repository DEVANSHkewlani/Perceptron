"""Tests for dashboard API connect gating and data endpoints."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dashboard import api as dashboard_api


@pytest.fixture
def client():
    dashboard_api.connection_requested = False
    dashboard_api.connection_errors = []
    dashboard_api.last_status = {}
    with TestClient(dashboard_api.app) as c:
        yield c
    dashboard_api.connection_requested = False


def test_events_empty_when_disconnected(client):
    r = client.get("/api/events")
    assert r.status_code == 200
    assert r.json() == []


def test_decision_none_when_disconnected(client):
    r = client.get("/api/decision/latest")
    assert r.status_code == 200
    assert r.json() is None


def test_graph_empty_when_disconnected(client):
    r = client.get("/api/graph/d3")
    assert r.status_code == 200
    assert r.json() == {"nodes": [], "links": []}


def test_connect_fails_without_shopcore(monkeypatch, client):
    monkeypatch.setattr(dashboard_api, "is_shopcore_connected", lambda: False)

    def fake_tcp(host, port, timeout=0.25):
        if port == 8010:
            return False, "--"
        return True, "1ms"

    monkeypatch.setattr(dashboard_api, "_tcp_check", fake_tcp)

    r = client.post("/api/connect")
    assert r.status_code == 503
    body = r.json()
    assert body["ready"] is False
    assert body["connected"] is False
    assert any("shopcore" in err for err in body["errors"])


def test_connect_succeeds_when_ready(monkeypatch, client):
    monkeypatch.setattr(dashboard_api, "is_shopcore_connected", lambda: True)
    monkeypatch.setattr(dashboard_api, "_tcp_check", lambda host, port, timeout=0.25: (True, "1ms"))

    r = client.post("/api/connect")
    assert r.status_code == 200
    body = r.json()
    assert body["ready"] is True
    assert body["connected"] is True


def test_disconnect_clears_connection(client, monkeypatch):
    monkeypatch.setattr(dashboard_api, "is_shopcore_connected", lambda: True)
    monkeypatch.setattr(dashboard_api, "_tcp_check", lambda host, port, timeout=0.25: (True, "1ms"))
    client.post("/api/connect")

    r = client.post("/api/disconnect")
    assert r.status_code == 200
    assert r.json()["connected"] is False

    status = client.get("/api/connection/status").json()
    assert status["connected"] is False
    assert status["requested"] is False


def test_extract_decision_nested():
    payload = {
        "decision": {
            "recommended_action": "restart_service",
            "confidence": 0.9,
            "situation_assessment": "test",
        }
    }
    decision = dashboard_api._extract_decision(payload)
    assert decision["recommended_action"] == "restart_service"


def test_extract_decision_flat():
    payload = {"recommended_action": "scale_service_horizontal", "confidence": 0.8}
    decision = dashboard_api._extract_decision(payload)
    assert decision["recommended_action"] == "scale_service_horizontal"
