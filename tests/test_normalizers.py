import pytest
from cognitive_perception.normalizers.log_normalizer import LogNormalizer
from cognitive_perception.normalizers.api_normalizer import APINormalizer
from cognitive_perception.normalizers.sensor_normalizer import SensorNormalizer
from cognitive_perception.normalizers.security_normalizer import SecurityEventNormalizer
from cognitive_perception.normalizers.metric_normalizer import MetricNormalizer
from cognitive_perception.schema.event import CognitiveEvent, PerceptionFailure

def test_log_db_timeout():
    result = LogNormalizer().normalize({
        "message": "connection to postgres-primary timed out after 5000ms",
        "level": "error", "service": "auth-service",
        "timestamp": "2024-01-15T14:23:11Z"
    }, "svc:auth-service")
    assert isinstance(result, CognitiveEvent)
    assert result.event_type == "database_connection_timeout"
    assert result.severity == "high"

def test_log_bad_input_returns_failure():
    result = LogNormalizer().normalize({"level": "info"}, "svc:auth-service")
    assert isinstance(result, PerceptionFailure)
    assert result.normalizer == "LogNormalizer"

def test_api_ssl_expiring():
    result = APINormalizer().normalize({
        "url": "https://example.com", "status_code": 200,
        "latency_ms": 100, "ok": True, "error": None,
        "ssl_days_remaining": 5, "latency_warn_ms": 500, "latency_high_ms": 2000
    }, "svc:example")
    assert result.event_type == "service_ssl_expiring"
    assert result.severity == "high"

def test_sensor_temperature_critical():
    result = SensorNormalizer().normalize({
        "sensor_type": "temperature", "value": 50.0,
        "location": "server-room",
        "thresholds": {"warn": 35.0, "critical": 45.0}
    }, "sensor:server-room-temp")
    assert result.event_type == "temperature_critical"
    assert result.severity == "critical"

def test_security_sql_injection():
    result = SecurityEventNormalizer().normalize({
        "type": "SQL_INJECTION", "ip": "1.2.3.4",
        "uri": "/api/users?id=1 OR 1=1", "country": "RU"
    }, "security:cloudflare")
    assert result.event_type == "sql_injection_attempt"
    assert "security:ip-1-2-3-4" in result.entity_refs

def test_metric_cpu_spike():
    result = MetricNormalizer().normalize({
        "labels": {"alertname": "HighCPUUsage", "instance": "node-01:9100", "job": "node"},
        "annotations": {"summary": "CPU above 85%"},
        "status": "firing", "startsAt": "2024-01-15T14:00:00Z"
    }, "metric:node-01")
    assert result.event_type == "cpu_spike"
    assert result.confidence == 0.98

def test_deterministic_id():
    from datetime import datetime, timezone
    from cognitive_perception.schema.event import CognitiveEvent, Severity, SourceType
    
    ts = datetime(2024, 1, 15, 14, 23, 11, tzinfo=timezone.utc)
    evt1 = CognitiveEvent(
        timestamp=ts,
        source_type=SourceType.LOG,
        source_id="svc:auth-service",
        event_type="database_connection_timeout",
        severity=Severity.HIGH,
        payload={"database": "postgres-primary"},
        entity_refs=["svc:auth-service", "db:postgres-primary"],
        confidence=0.82,
        tags=["log", "database"]
    )
    
    evt2 = CognitiveEvent(
        timestamp=ts,
        source_type=SourceType.LOG,
        source_id="svc:auth-service",
        event_type="database_connection_timeout",
        severity=Severity.HIGH,
        payload={"database": "postgres-primary"},
        entity_refs=["svc:auth-service", "db:postgres-primary"],
        confidence=0.82,
        tags=["log", "database"]
    )
    
    assert evt1.event_id.startswith("evt_")
    assert len(evt1.event_id) == 16
    assert evt1.event_id == evt2.event_id

def test_ingestion_lag():
    from datetime import datetime, timezone, timedelta
    from cognitive_perception.normalizers.log_normalizer import LogNormalizer
    
    ten_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    result = LogNormalizer().normalize({
        "message": "connection to postgres-primary timed out after 5000ms",
        "level": "error", "service": "auth-service",
        "timestamp": ten_min_ago
    }, "svc:auth-service")
    
    assert isinstance(result, CognitiveEvent)
    assert "_ingestion_lag_s" in result.payload
    assert result.payload["_ingestion_lag_s"] >= 600.0
    assert "very_stale" in result.tags
    assert "stale_event" in result.tags