"""
Source Configuration
====================
YAML manifest loader and source registry.
This is Method 1 (config file) from the source registration system.

HOW TO USE:
1. Edit sources.yaml to list your sources
2. The perception layer reads this on startup
3. One adapter instance is created per source entry
"""

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SourceConfig:
    """A single source entry from the YAML manifest."""
    id: str
    type: str           # matches SourceType enum values
    enabled: bool = True
    tags: list[str] = field(default_factory=list)
    # Type-specific fields stored here
    params: dict[str, Any] = field(default_factory=dict)


def load_sources(config_path: str = "sources.yaml") -> list[SourceConfig]:
    """Load source configs from YAML manifest."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Source config not found: {config_path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    sources = []
    for entry in data.get("sources", []):
        src_type = entry.pop("type")
        src_id   = entry.pop("id")
        enabled  = entry.pop("enabled", True)
        tags     = entry.pop("tags", [])

        sources.append(SourceConfig(
            id=src_id,
            type=src_type,
            enabled=enabled,
            tags=tags,
            params=entry,
        ))

    return [s for s in sources if s.enabled]


# ─────────────────────────────────────────────
# EXAMPLE SOURCES.YAML (write this to disk)
# ─────────────────────────────────────────────

EXAMPLE_SOURCES_YAML = """
# ============================================================
# Cognitive Perception — Source Manifest
# ============================================================
# One entry per signal source. The perception layer reads this
# on startup and instantiates one adapter per enabled source.
#
# source_id naming convention (must match entity namespace):
#   Services:   svc:service-name
#   Databases:  db:database-name
#   Queues:     queue:queue-name
#   Files:      file:path-description
#   Metrics:    metric:host-or-job-name
#   Users:      usr:user-id  (generated at runtime, not in config)
#   Agents:     agent:agent-id
#   Sensors:    sensor:sensor-name

sources:

  # ──────────────────────────────────────────────────────────
  # 1. APPLICATION LOGS  (source_type: log)
  # Fluent Bit tails these and forwards to Kafka.
  # The normalizer picks them up from the kafka topic.
  # ──────────────────────────────────────────────────────────
  - id: svc:auth-service
    type: log
    tags: [production, critical, auth]
    fluent_bit_tag: auth-service
    log_format: json         # json | plaintext | logfmt

  - id: svc:api-gateway
    type: log
    tags: [production, critical, gateway]
    fluent_bit_tag: api-gateway
    log_format: json

  - id: svc:order-service
    type: log
    tags: [production, standard, orders]
    fluent_bit_tag: order-service
    log_format: plaintext

  # ──────────────────────────────────────────────────────────
  # 2. SYSTEM METRICS  (source_type: metric)
  # Prometheus scrapes these. Alertmanager sends webhooks to us.
  # No per-source adapter needed — the webhook endpoint handles all.
  # The entries below configure which Prometheus jobs we care about.
  # ──────────────────────────────────────────────────────────
  - id: metric:auth-service
    type: metric
    tags: [production, critical]
    prometheus_job: auth-service
    alert_prefix: AuthService    # matches your Prometheus rule prefixes

  - id: metric:node-01
    type: metric
    tags: [infrastructure]
    prometheus_job: node-exporter
    alert_prefix: Node

  - id: metric:k8s
    type: metric
    tags: [infrastructure, kubernetes]
    prometheus_job: kube-state-metrics

  # ──────────────────────────────────────────────────────────
  # 3. APIs & SERVICES  (source_type: api)
  # Polling adapter — checks health endpoints on schedule.
  # ──────────────────────────────────────────────────────────
  - id: svc:auth-service
    type: api
    tags: [production, critical, internal]
    url: http://localhost:8080/health
    poll_interval_s: 15
    timeout_s: 3.0
    latency_warn_ms: 200
    latency_high_ms: 1000

  - id: svc:api-gateway
    type: api
    tags: [production, critical, internal]
    url: http://localhost:8000/health
    poll_interval_s: 15
    timeout_s: 3.0

  - id: ext:stripe-api
    type: api
    tags: [external, payments, critical]
    url: https://api.stripe.com/v1/     # returns 401 but confirms reachability
    poll_interval_s: 60
    timeout_s: 5.0

  # ──────────────────────────────────────────────────────────
  # 4. DATABASES  (source_type: database)
  # Direct asyncpg monitoring.
  # ──────────────────────────────────────────────────────────
  - id: db:postgres-primary
    type: database
    tags: [production, critical, postgres]
    dsn: postgresql://postgres:postgres@localhost:5432/postgres
    slow_query_ms: 100.0
    pool_warn_pct: 0.70
    pool_critical_pct: 0.90
    replication_lag_warn_s: 30.0
    lock_wait_warn_s: 5.0
    poll_interval_s: 15

  # ──────────────────────────────────────────────────────────
  # 5. QUEUES  (source_type: queue)
  # Queue monitor polls broker admin API on schedule.
  # ──────────────────────────────────────────────────────────
  - id: queue:order-events
    type: queue
    tags: [production, orders]
    broker_type: kafka
    queue_name: order-events
    consumer_group: order-processor
    poll_interval_s: 15
    kafka_bootstrap: localhost:9092

  # ──────────────────────────────────────────────────────────
  # 6. FILESYSTEM  (source_type: file)
  # inotify watches these paths. Events arrive in real time.
  # ──────────────────────────────────────────────────────────
  - id: file:nginx-config
    type: file
    tags: [config, production]
    watch_path: ./files/nginx/conf.d/
    events: [create, modify, delete]
    recursive: false

  - id: file:ssl-certs
    type: file
    tags: [security, certificates, critical]
    watch_path: ./files/ssl/certs/
    events: [create, modify, delete]
    cert_expiry_warn_days: 30

  # ──────────────────────────────────────────────────────────
  # 7. SENSORS (source_type: sensor)
  # IoT adapter — connects via MQTT or polls/pushes REST.
  # ──────────────────────────────────────────────────────────
  - id: sensor:server-room-temp
    type: sensor
    tags: [infrastructure, environment]
    protocol: HTTP_POLL
    endpoint: http://localhost:8001/sensors/temp
    sensor_type: temperature
    thresholds:
      critical_high: 28.0
      warn_high: 24.0
    poll_interval_s: 10
"""


def write_example_config(path: str = "sources.yaml"):
    """Write the example YAML to disk if it doesn't exist."""
    p = Path(path)
    if not p.exists():
        p.write_text(EXAMPLE_SOURCES_YAML.strip())
        print(f"Created example source config: {path}")
    else:
        print(f"Source config already exists: {path}")
