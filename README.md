# Perceptron — Cognitive Perception Layer

## Overview

`Perceptron` is Phase 1 of a larger digital cognitive architecture. It transforms raw operational signals from logs, metrics, APIs, databases, queues, files, browser clients, and security webhooks into a unified structured event format called `CognitiveEvent`.

This layer is built to:
- normalize multi-source telemetry into a single schema
- publish events to a Kafka-compatible message bus
- capture failures explicitly for debugging
- serve as the foundation for later memory, knowledge graph, and reasoning phases

## Key Features

- Polls service health endpoints, database metrics, queue state, Redis state, and filesystem changes
- Consumes log input through Fluent Bit / Redpanda
- Accepts push/webhook ingestion for browser events, Prometheus alerts, security events, and agent-generated events
- Enforces strict event contract validation with Pydantic
- Emits normalized events to Kafka topics
- Writes ingestion failures to a dedicated diagnostics topic

## Repository Structure

- `perception_main.py` — orchestrator that loads `sources.yaml` and starts configured adapters
- `sources.yaml` — source manifest defining observed signal sources
- `docker-compose.yml` — local service stack including Redpanda, Redis, Prometheus, Alertmanager, Mosquitto, TimescaleDB, Neo4j, and Qdrant
- `requirements.txt` — Python package dependencies
- `cognitive_perception/` — core package implementing adapters, normalizers, schema, and the push API
- `sdk/` — browser SDK entrypoint for front-end user and browser event capture
- `infrastructure/` — telemetry and alerting configuration files

## Core Components

### `cognitive_perception/perception_api/main.py`

FastAPI service that handles direct HTTP ingestion from:
- browser user events
- browser environment events
- Prometheus Alertmanager webhooks
- security event webhooks
- agent-generated events

### `perception_main.py`

Entrypoint for polling-based adapters. It loads `sources.yaml` and starts active adapter processes:
- `APIAdapter`
- `LogAdapter`
- `DatabaseAdapter`
- `QueueAdapter`
- `FileAdapter`
- `RedisAdapter`

### Event topics

- `cognitive.events` — primary normalized event stream
- `cognitive.perception_failures` — ingestion failures and validation diagnostics

## Getting Started

### Prerequisites

- Python 3.10+
- Docker
- Docker Compose
- `pip`

### Start Infrastructure

From the project root:

```bash
docker compose up -d
```

> Note: `docker-compose.yml` requires an external Docker network named `target_system_shopcore`. If that network does not exist, create it before starting:

```bash
docker network create target_system_shopcore
```

### Install Python dependencies

```bash
pip install -r requirements.txt
```

### Start the Perception API

```bash
uvicorn cognitive_perception.perception_api.main:app --host 0.0.0.0 --port 8080 --reload
```

### Start the Perception Orchestrator

```bash
python perception_main.py
```

### Verify the event stream

Open Redpanda Console at `http://localhost:8888` and inspect the `cognitive.events` topic.

## Configuration

### `sources.yaml`

This manifest defines the observed signal sources. Supported source types include:
- `log`
- `api`
- `database`
- `queue`
- `file`
- `redis`
- `sensor`
- `metric`

The included example manifest contains ShopCore service monitors, database polling, Redis health checks, queue lag observations, and filesystem watchers.

### Environment variables

The project supports runtime overrides:
- `KAFKA_BOOTSTRAP_SERVERS` — default: `localhost:9092`
- `REDIS_URL` — default: `redis://localhost:6379`

## Push API Endpoints

The FastAPI application exposes these endpoints:
- `POST /perception/user-events`
- `POST /perception/browser-events`
- `POST /perception/prometheus-alerts`
- `POST /perception/security-events`
- `POST /perception/agent-events`

## Runtime Behavior

- Incoming raw signals are normalized by dedicated normalizers.
- Valid events are published to `cognitive.events`.
- Validation failures are published to `cognitive.perception_failures`.
- Polling adapters use Redis and fingerprinting to reduce event noise.

## Useful Commands

```bash
docker compose ps
docker compose logs -f redpanda prometheus alertmanager redis
uvicorn cognitive_perception.perception_api.main:app --host 0.0.0.0 --port 8080
python perception_main.py
```

## Development

- Add adapters under `cognitive_perception/adapters`
- Add normalizers under `cognitive_perception/normalizers`
- Update `sources.yaml` to enable or configure signal sources
- Add new push ingestion routes in `cognitive_perception/perception_api/main.py`

## Testing

Run Python tests with:

```bash
pytest
```

## Notes

- This repository is designed as the perception foundation for a larger AI-driven system.
- It emphasizes structured normalization, explicit failure handling, and observable event delivery.
- The current stack is built around Redpanda/Kafka, Redis, FastAPI, and Pydantic validation.

## License

Add your license information here.
