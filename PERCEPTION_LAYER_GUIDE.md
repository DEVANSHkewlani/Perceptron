# 📖 Operational & Architectural Manual: Cognitive Perception Layer

---

## 1. Core Philosophy: reality vs perception

In this digital cognitive architecture, we draw a strict line between **Reality** (what the systems emit) and **Perception** (how the mind interprets it). 

- **Reality is chaotic, fragmented, and multi-modal.** It consists of arbitrary plaintext logs, Prometheus metrics, Postgres connection pools, MQTT sensor values, inotify filesystem blocks, and browser events.
- **Perception is ordered, semantic, and unified.** It translates this multi-modal chaos into a single data structure: the `CognitiveEvent`.

To achieve this, the architecture splits the problem into two distinct stages:
1. **Adapters (I/O Connectors)**: Connect to physical environments (polling APIs, consuming Kafka feeds, watching files, subscribing to MQTT). They are **meaning-agnostic**. Their only job is to transport raw data safely.
2. **Normalizers (Semantic Translators)**: Receive raw data from adapters, extract context, parse messages, run regex classifiers, evaluate boundaries, and assign meaning. They are **I/O-free**. They take a dictionary and return a validated `CognitiveEvent` or a `PerceptionFailure`.

By separating transport from interpretation, you can easily change how you monitor a service (e.g., switching a database poller to an HTTP exporter) without modifying the downstream cognitive reasoning engine.

---

## 2. The Schema Layer (`schema/`)

The schema acts as the unified contract for the entire architecture. If perception produces a corrupt event, all downstream reasoning engines, world models, and memory systems fail. 

### 1. `CognitiveEvent` (in `schema/event.py`)
A strict Pydantic model enforcing the unified data contract:

| Field | Type | Functioning & Rationale |
| :--- | :--- | :--- |
| `event_id` | `str` | Prefixed with `evt_` and followed by a unique UUID. Used for **distributed tracing and deduplication** across downstream queues. |
| `timestamp` | `datetime` | The **moment the event actually occurred** in reality (e.g., when the log line was printed). Essential for chronological sorting, as networks can deliver events out of order. |
| `ingested_at` | `datetime` | The moment the Perception Layer received the event. The difference `ingested_at - timestamp` measures **ingestion lag** (telemetry latency). |
| `source_type` | `SourceType` | Strict enum matching the adapter pipeline (`log`, `metric`, `api`, `database`, `queue`, `file`, `user_event`, `browser_event`, `security_event`, `sensor`, `agent_event`). |
| `source_id` | `str` | Strict namespaced string representing the physical asset (e.g., `svc:auth-service`). Allows Phase 3 Knowledge Graph to attach events directly to physical nodes. |
| `event_type` | `str` | The **semantic label** (e.g., `database_connection_timeout`). This is the primary key used by the Reasoning Engine to trigger rules. |
| `severity` | `Severity` | Strict Enum (`info`, `low`, `medium`, `high`, `critical`). Controls routing priority in the Phase 2 Memory Router. |
| `payload` | `dict` | Fully structured dictionary containing contextual variables (never raw strings). |
| `entity_refs` | `list[str]` | **The most critical field.** Lists namespaced entities touched by this event (e.g., `["svc:auth-service", "db:postgres-primary"]`). The Knowledge Graph reads this list to automatically draw or strengthen edges between those nodes. |
| `confidence` | `float` | Certainty rating from `0.0` to `1.0`. Metrics have high certainty (`0.99`), whereas log parsing is lower (`0.80`) due to regex matching limits. |
| `tags` | `list[str]` | Metadata tags (e.g., `["production", "auth", "slow_query"]`) for flexible routing. |

### 2. Entity Namespaces
To keep the Knowledge Graph unified, `source_id` and `entity_refs` must strictly match the entity namespaces validated by Pydantic:
- `svc:<name>`: A service or application process (e.g., `svc:api-gateway`).
- `db:<name>`: A database host or cluster (e.g., `db:postgres-primary`).
- `queue:<name>`: A message queue topic or broker (e.g., `queue:order-events`).
- `file:<path>`: A watched configuration or secret file.
- `metric:<node>`: A host or container node generating metrics (e.g., `metric:node-01`).
- `usr:<id>`: A unique user session (e.g., `usr:user-10492`).
- `browser:<id>`: A client browser session.
- `security:<source>`: A firewall, WAF, or blocker system.
- `sensor:<name>`: An IoT or environmental physical sensor.
- `agent:<id>`: A cognitive reasoning sub-agent.
- `ext:<name>`: An external third-party API (e.g., `ext:stripe`).

### 3. `PerceptionFailure` (in `schema/event.py`)
If validation or parsing fails, **we never drop the signal silently.** We construct a `PerceptionFailure` containing:
- `raw_input`: The original corrupt or unparsed payload.
- `failure_reason`: The error message or validation traceback.
- `normalizer_name`: The normalizer class that failed.
- `failed_at`: Ingestion time of the failure.

These are shipped to a dedicated Kafka topic `cognitive.perception_failures` for real-time monitoring and alerting.

### 4. `event_types.py` (The Vocabulary)
Houses a catalog of ~130 strict, immutable `event_type` string constants organized into 10 categories (e.g., `LOG_EVENTS`, `DATABASE_EVENTS`, `USER_EVENTS`). 
- **Rationale**: If developer A writes `"database_timeout"` and developer B writes `"db_connection_refused"`, the downstream reasoning engine cannot perform reliable correlations. This file acts as the singular vocabulary dictionary. String literals are never hardcoded inside normalizers—they must be imported from `event_types.py`.

---

## 3. The Adapter Layer (`adapters/`)

Adapters are the I/O workforce. They handle network connections, scheduling, thread safety, and queuing.

```
       ADAPTER TYPES AND THEIR I/O TRANSPORT MODELS:

  ┌──────────────┐
  │  APIAdapter  │ ── Scheduled Async Http Client (httpx) ────→ Redis Fingerprint
  └──────────────┘
  ┌──────────────┐
  │  LogAdapter  │ ── Kafka Consumer (aiokafka) ──────────────→ Partition Consumer Group
  └──────────────┘
  ┌──────────────┐
  │  DBAdapter   │ ── Direct pg Connection (asyncpg) ─────────→ PG System Tables Poll
  └──────────────┘
  ┌──────────────┐
  │  FileAdapter │ ── OS watchdogs in threads ────────────────→ Threadsafe Async Queue
  └──────────────┘
  ┌──────────────┐
  │ SensorAdapter│ ── MQTT Broker Subscription (aiomqtt) ─────→ Async Push Telemetry
  └──────────────┘
```

### 1. `api_adapter.py`
- **Functioning**: Continuously polls a list of URLs (e.g., `/health` status endpoints). It measures latency and maps status codes.
- **Change Detection**: To prevent event storms, it hashes the response status and latency into a fingerprint (`"{ok}:{status_code}:{latency_bucket}"`) and compares it against Redis. It only publishes to the event bus when this fingerprint *changes* (e.g., status flips from `200` to `503`, or latency goes from `normal` to `slow`).
- **Security Check**: For `https://` targets, it automatically initializes a SSL default context, wraps a raw socket, and reads the certificate's `notAfter` metadata to calculate remaining days before expiration.

### 2. `log_adapter.py`
- **Functioning**: Runs a background Kafka consumer listening to the `raw.logs` topic (populated by log shippers like Fluent Bit). 
- **Decoupled Commits**: To guarantee delivery, it uses manual commits. It only commits the raw log partition offset to Redpanda *after* the normalizer has successfully completed and the event is safely routed.
- **Tracing extraction**: Inspects JSON logs to find `trace_id`, `request_id`, `correlation_id`, or `span_id`, appending them as tags to link logs across microservice boundaries.

### 3. `database_adapter.py`
- **Functioning**: Directly connects to Postgres clusters using `asyncpg` to poll system catalog tables that standard Prometheus exporters cannot access.
- **Queries**:
  - `pg_stat_activity`: Checks for long-running queries exceeding thresholds, and processes holding connection states as `idle in transaction` (detecting connection leaks).
  - `pg_stat_replication`: Calculates byte lags and reply delays on replica standbys.
  - `pg_locks` join: Discovers query blocking chains (identifies blocking and blocked process IDs).

### 4. `file_adapter.py`
- **Functioning**: Subscribes to filesystem kernel events using Python `watchdog` (utilizing `inotify` on Linux and `fsevents` on macOS).
- **Thread Bridge**: Since `watchdog` observers block and run inside native OS threads, they cannot interact with async loops. The adapter handles this by queueing events, and bridging them into the async loop thread-safely via `loop.call_soon_threadsafe(queue.put_nowait, event)`.
- **Specialized parsing**: If a watched file matches secret keywords, it escalates the severity. If it has a cert extension (`.pem`, `.crt`), it parses its SSL structure.

### 5. `queue_adapter.py`
- **Functioning**: Polls broker admin endpoints. It accesses Kafka Admin APIs or HTTP REST Management APIs for RabbitMQ.
- **Decoupling**: Returns a unified `QueueMetrics` dataclass that represents consumer lag and queue depths, completely decoupling the normalizer from broker-specific networking.

### 6. `sensor_adapter.py`
- **Functioning**: Ingests IoT device telemetry. Connects via two pipelines:
  - `MQTT`: Establishes an async connection to an MQTT broker (like Mosquitto) and subscribes to topic hierarchies (e.g., `sensors/#`).
  - `HTTP_POLL`: Scheduled GET poller for RESTful devices.

---

## 4. The Normalizer Layer (`normalizers/`)

Normalizers contain zero I/O or network code. They represent the "brain" of the ingestion pipelines, transforming raw maps into structured semantic events.

### 1. `base.py` (`BaseNormalizer`)
- **Role**: Employs the **Template Method Pattern**. It provides the public `normalize` method which wraps the abstract `_normalize` implementation in a robust `try/except` block, guaranteeing that failures always return a `PerceptionFailure` rather than crashing the calling thread.
- **Timestamp parsing**: Standardizes any date format (Unix epoch milliseconds, ISO 8601, RFC 3339) into a timezone-aware UTC datetime.

### 2. `log_normalizer.py`
- **Parsing Flow**: Iterates through a library of pre-compiled regex patterns.
- **Regex Library**: Maps strings (e.g., `"connection pool exhausted"`) to events like `database_connection_timeout`.
- **Confidence**: Set between `0.80` and `0.88` due to the heuristic nature of matching unstructured text.

### 3. `metric_normalizer.py`
- **Parsing Flow**: Maps Alertmanager alert payloads.
- **Rule Maps**: Translates alert names (e.g., `HighCPUUsage`) to events like `cpu_spike`.
- **Confidence**: Highly deterministic (`0.98`), since alerting thresholds are backed by strict mathematical logic in Prometheus.

### 4. `user_normalizer.py`
- **Parsing Flow**: Processes user actions in web applications.
- **Behavior Mapping**: Standardizes client-side event strings (e.g., `checkout_start`) to strict vocabulary constants (`checkout_started`).
- **Classification**: Assigns categories like `frustration` for rage clicks, or `conversion` for checkout events.

### 5. `browser_normalizer.py`
- **Parsing Flow**: Checks Web Vitals values against Google thresholds:
  - `LCP > 4000ms` -> `lcp_poor` (High Severity).
  - `LCP 2500-4000ms` -> `lcp_needs_improvement` (Medium Severity).
- **Error classification**: Inspects error text to identify specific failure conditions (e.g., matches React SSR bugs as `hydration_failure`, or code-splitting errors as `js_chunk_load_failed`).

### 6. `security_normalizer.py`
- **Parsing Flow**: Maps security events from firewalls, WAFs, or Auth servers.
- **Attack Classification**: Standardizes raw actions into types like `sql_injection_attempt`, `brute_force_detected`, or `ddos_detected`.
- **Entity creation**: Enriches references with IP networks (`security:ip-127-0-0-1`) to enable downstream IP reputation tracking in the Knowledge Graph.

---

## 5. Webhook Ingestion API (`perception_api/main.py`)

A high-performance FastAPI server configured to capture push-based real-time telemetry.

```
       FASTAPI WEBHOOK INGESTION ENGINE:
       
  Client Push   ── POST ──→  FastAPI Endpoint ─┐
                                              │ (Validate Pydantic model)
                                              ▼
                             Enqueue BackgroundTask ──→ Immediate 202 Accepted
                                              │
                                              ▼
                                   Resolve Domain Normalizer
                                              │
                                              ▼
                                    Kafka / Redpanda
                               (Topic: cognitive.events)
```

- **Asynchronous Lifespan**: Boots an `AIOKafkaProducer` on startup. The producer is reused across all requests, eliminating connection setup overhead.
- **Non-blocking Telemetry**: Ingests payloads from clients, executes rapid Pydantic validation, schedules processing as a FastAPI `BackgroundTask`, and returns an immediate `202 Accepted` response. This ensures client applications never block on event ingestion.
- **Normalizer Routing**: The background threads pass payloads to class-based normalizers (`UserBehaviorNormalizer`, `BrowserEventNormalizer`, `SecurityEventNormalizer`), ensuring that WAF, JS SDK, and Alertmanager data are correctly translated and written to the `cognitive.events` and `cognitive.perception_failures` topics.

---

## 6. Zero-Dependency Browser Web SDK (`sdk/cognitive_sdk.js`)

A lightweight client tracking script loaded in client browsers.

- **Asynchronous Batching**: Intercepts user interactions (page views, interactive clicks, form submissions) and pushes them to a local queue. A scheduled background timer flushes this queue to `/perception/user-events` every 5 seconds, minimizing network request volume.
- **Rage Click Engine**: Implements click tracking. If the user clicks the same DOM node 3 times within 800 milliseconds, it records a `rage_click` event, classifying user frustration in real time.
- **Core Web Vitals Tracker**: Instantiates a `PerformanceObserver` targeting browser performance entries (`largest-contentful-paint`, `first-input`, `layout-shift`). It calculates LCP, CLS, and FID values and immediately sends poor scores to the `/perception/browser-events` endpoint.
- **Exception Trap**: Hooks into the browser's global `window.onerror` and `window.unhandledrejection` callbacks to capture JavaScript errors and stack traces.
- **Clean Shutdown**: Listens to the `beforeunload` event. On page exit, it uses `navigator.sendBeacon` to flush any remaining queued events to the server synchronously, ensuring no telemetry is lost.

---

## 7. The Scheduled Orchestrator (`perception_main.py`)

The main runtime execution engine of the Perception Layer.

- **Bootstrapping**: Loads `sources.yaml` on startup using the config module. For each enabled source, it instantiates the corresponding configuration dataclass (e.g., `APISourceConfig`).
- **Concurrency**: Gathers all adapter `run` routines (e.g., `APIAdapter.run()`, `LogAdapter.run()`) and operates them concurrently in a single `asyncio` event loop.
- **Fault-Tolerant Supervisors**: Wraps every adapter execution loop in a supervisor function (`run_adapter_safe`). If an adapter crashes (due to a database connection timeout or a broker disconnect), the supervisor:
  1. Catches the error and logs the traceback.
  2. Applies an **exponential backoff delay** (starting at 2s, doubling to a maximum of 60s).
  3. Re-instantiates and restarts the adapter, keeping the rest of the perception system running smoothly.

---

## 8. Multi-Container Telemetry Environment (`infrastructure/`)

A pre-configured Docker Compose environment providing all dependencies needed to run the Perception Layer.

- **Redpanda**: Orchestrates the message bus, operating ZooKeeper-free and starting in under a second.
- **Redpanda Console**: Visual web interface on port `8888` allowing real-time inspection of topics, consumer groups, schemas, and event streams.
- **Redis Cache**: Used by API and queue polling adapters to manage change-detection state.
- **Mosquitto**: An MQTT broker that ingests raw telemetry streams from IoT sensors.
- **Fluent Bit**: Mounts the host application log directory (`/var/log/cognitive/`) and tails logs in real time, shipping lines as structured JSON to Redpanda's `raw.logs` topic.
- **Prometheus**: Scrapes operational metrics from active jobs. If rules inside `alert.rules.yml` evaluate to true (e.g., `process_cpu_seconds_total > 0.85`), it triggers an alert.
- **Alertmanager**: Receives active alert alerts from Prometheus, groups them, and routes them via HTTP POST to the local Perception API webhook endpoint.

---

## 9. End-to-End Operational Signal Flows

### Flow A: The Life of an Application Log

```
 1. APPLICATION LOG EMITTED
    An application writes a structured JSON log line to /var/log/cognitive/app.log:
    {"message": "connection to postgres-primary timed out...", "level": "error", "tag": "auth-service"}
                                 │
                                 ▼
 2. FLUENT BIT TAILING & SHIPPING
    Fluent Bit tails /var/log/cognitive/app.log in real time. It enriches the record
    with host metadata and publishes the raw JSON payload to the "raw.logs" Redpanda topic.
                                 │
                                 ▼
 3. LOG ADAPTER CONSUMPTION
    The LogAdapter consumer group reads the partition offset from "raw.logs".
    It extracts the Fluent Bit tag ("auth-service") and maps it to the source entity ID: "svc:auth-service".
                                 │
                                 ▼
 4. LOG NORMALIZATION
    The LogAdapter passes the raw log message string to the LogNormalizer.
    The normalizer parses the log, matches it against pre-compiled regex patterns,
    extracts entity references ("db:postgres-primary"), and returns a validated CognitiveEvent.
                                 │
                                 ▼
 5. EVENT BUS ROUTING
    The LogAdapter receives the CognitiveEvent, enriches it with trace tags,
    and publishes the event payload to the "cognitive.events" topic.
    It then commits the partition offset back to Redpanda.
```

---

### Flow B: The Life of a Metric Alert

```
 1. PROMETHEUS SCRAPING & EVALUATION
    Prometheus scrapes system metrics every 15s. An alert rule (e.g., CPU > 85%) evaluates to true.
    Prometheus raises the alert state to "firing" and pushes the payload to Alertmanager.
                                 │
                                 ▼
 2. ALERTMANAGER DISPATCH
    Alertmanager receives the alert group, applies routing rules, and dispatches the alert payload
    via an HTTP POST request to the local Perception API webhook endpoint.
                                 │
                                 ▼
 3. PERCEPTION API INGESTION
    The FastAPI Webhook router receives the payload on "/perception/prometheus-alerts",
    validates the structure, schedules processing in a BackgroundTask, and returns "202 Accepted".
                                 │
                                 ▼
 4. METRIC NORMALIZATION
    The background task passes each alert from the payload to the MetricNormalizer.
    The normalizer maps the alert name to its event type ("cpu_spike"), extracts node entities
    (e.g., "metric:node-01"), and builds a validated CognitiveEvent.
                                 │
                                 ▼
 5. EVENT BUS ROUTING
    The Perception API's AIOKafkaProducer publishes the validated event payload to the
    "cognitive.events" topic, making it immediately available to downstream reasoning layers.
```
