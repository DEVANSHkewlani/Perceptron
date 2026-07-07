# Perceptron — Decentralized Closed-Loop Cognitive Architecture (DCA)

Perceptron is a complete, multi-tiered autonomic self-healing cognitive architecture. Rather than relying on simple linear alerting or isolated LLM agents, it implements a **continuous, closed-loop cognitive cycle**—sensing, memory routing, situational modeling, temporal analysis, logical reasoning, multi-step planning, parallel execution, and feedback reinforcement.

---

## 📖 Table of Contents
1. [Core Philosophy](#-core-philosophy)
2. [Why This Architecture is Better](#-why-this-architecture-is-better)
3. [System Architecture Flowchart](#-system-architecture-flowchart)
4. [The 10 Cognitive Layers](#-the-10-cognitive-layers)
5. [E2E Lifecycle Example: Self-Healing in Action](#-e2e-lifecycle-example-self-healing-in-action)
6. [Getting Started & Local Execution](#-getting-started--local-execution)
7. [Directory Map](#-directory-map)

---

## 🧠 Core Philosophy

Perceptron operates under two fundamental tenets:

1. **Reality vs. Perception:** 
   * *Reality* (the physical environment) is chaotic, noisy, and multi-modal (containing raw text logs, Prometheus metrics, DB connection stats, MQTT values, WAF webhooks, and client browser clicks).
   * *Perception* is clean, structured, and unified. It transforms physical telemetry into a single data contract: the `CognitiveEvent` schema. By decoupling ingestion adapters from interpretation normalizers, the reasoning engine never has to parse log formats directly.
2. **Closed-Loop Autonomic Regulation:**
   * Traditional monitoring is open-loop: a threshold is breached, a human is page-alerted, and they perform manual root-cause analysis and remediation.
   * Perceptron closes the loop. It continuously perceives the system state, reasoning about root causes based on topology and experiences, generating plans, running interventions, and then checking if the system health improved—learning from failures and successes to update its operational playbooks automatically.

---

## 🚀 Why This Architecture is Better

Compared to traditional monitoring systems or standard single-agent LLM wrappers, Perceptron's multi-agent closed-loop design offers significant advantages:

| Dimension | Traditional Monitoring (APM / Dashboards) | Basic LLM Agent (Wrapper) | Perceptron (DCA) |
| :--- | :--- | :--- | :--- |
| **Action Cycle** | **Manual:** Alerts humans; relies on operators typing terminal commands. | **Brittle:** Run-loop calls LLM directly to write scripts on the fly. | **Autonomic:** Structured plans generated from certified vocabularies. |
| **Cost & Speed** | **Fast & Cheap:** But requires continuous human attention & triage. | **Slow & Expensive:** Every metric spike triggers costly LLM API calls. | **Hybrid (Fast/Slow):** Fast-Path resolves issues in ms; Slow-Path reasons on novel events. |
| **Memory Capacity** | **Raw Storage:** Database stores history, but agent has no contextual recall. | **Context Swamped:** Massive prompt injection dumps raw logs, hitting token limits. | **Multi-Tier Memory:** Segmented into working (Redis), episodic (Postgres), and semantic (Neo4j). |
| **Safety & Conflict** | **None:** Human coordinates; system cannot prevent conflicting actions. | **Dangerous:** Dual agents might run conflicting overrides concurrently. | **Conflict Resolver:** Graph-based topology queries check overlaps before action. |
| **Self-Improvement** | **Static:** Runbooks are updated manually by engineers writing wiki pages. | **Static Prompt:** Prompt templates remain unchanged unless edited by hand. | **Feedback Loop:** reinforcement learning updates playbook confidence. |

---

## 📊 System Architecture Flowchart

```
                 Telemetry Sources (Logs, Metrics, HTTP APIs, DBs, Queues)
                                             │
                                             ▼
                               ┌──────────────────────────┐
                               │     Perception Layer     │ 
                               │   (Adapters & Normalizers)│
                               └──────────────────────────┘
                                             │
                         Publishes validated "CognitiveEvent"s
                                             │
                                             ▼
                               ┌──────────────────────────┐
                ┌─────────────►│      Memory Router       │◄─────────────┐
                │              └──────────────────────────┘              │
                │                            │                           │
    Filters Stale Events            Parallel Ingestion              Feedback Updates
        (lag < 120s)                         │                       Playbooks & Graph
                │                            ▼                           │
   ┌───────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────┐
   │    Working Memory     │   │     Episodic Memory      │   │  Procedural Memory   │
   │ (Redis Active State)  │   │  (TimescaleDB History)   │   │  (Rules & Playbooks)  │
   └───────────────────────┘   └──────────────────────────┘   └──────────────────────┘
               ▲                             ▲                           ▲
               │                             │                           │
               └───────────────┐             │             ┌─────────────┘
                               ▼             ▼             ▼
                       ┌──────────────────────────────────────────┐
                       │               World Model                │◄─── [Temporal Engine]
                       │   (Situation Assessment & Neo4j Graph)    │   (Predictive Sequences)
                       └──────────────────────────────────────────┘
                                             │
                                   Triggers Monitor Agent
                                             │
                                             ▼
                               ┌──────────────────────────┐
                               │     Reasoning Engine     │ 
                               │   (Fast-Path / Slow-Path)│
                               └──────────────────────────┘
                                             │
                                 Formulates Action Suggestion
                                             │
                                             ▼
                               ┌──────────────────────────┐
                               │     Planning System      │
                               │  (Validates Actions/DAGs)│
                               └──────────────────────────┘
                                             │
                                    Dispatches Plan Tasks
                                             │
                                             ▼
                               ┌──────────────────────────┐
                               │     Execution Layer      │
                               │   (Async Action Handlers)│
                               └──────────────────────────┘
                                             │
                                 Evaluates Post-Health State
                                             │
                                             ▼
                               ┌──────────────────────────┐
                               │      Feedback Loop       │───────(Records result to Memory)
                               └──────────────────────────┘
```

---

## 🧩 The 10 Cognitive Layers

Perceptron is composed of ten distinct, decoupled services designed to run concurrently.

### 1. Perception Layer
* **Role:** Telemetry collection, schema validation, and noise reduction.
* **Core Components:**
  * [perception_main.py](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/perception_main.py): The polling orchestrator.
  * [cognitive_perception/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/cognitive_perception): Adapters and Pydantic normalizers.
* **Mechanism:** Polls sources (APIs, logs, DBs, queues) defined in [sources.yaml](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/sources.yaml). It uses **Redis hashing** to fingerprint responses; it only publishes a `CognitiveEvent` to the Kafka-compatible topic (`cognitive.events`) when the system state actually *changes* (preventing telemetry storms). 

### 2. Memory Tier
* **Role:** Specialized storage answering different cognitive recall queries.
* **Core Components:** [memory/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/memory)
* **Mechanisms:**
  * **Working Memory (Redis):** Stores active, transient state. It employs a **Staleness Audit**: if an incoming high-severity event has a telemetry lag of $\ge 120$ seconds, it is skipped to prevent polluting current state context.
  * **Episodic Memory (TimescaleDB/PostgreSQL):** Stores chronological logs of all events and agent decisions.
  * **Semantic Memory (Neo4j COMMUNITY):** Graph representation of services, dependencies, and resources. Links nodes dynamically based on `entity_refs`.
  * **Procedural Memory (PostgreSQL):** Houses rules, playbooks, mitigation records, and historical success rates.
  * **Vector Memory (Qdrant):** Embeds high-severity events for similarity searches during reasoning.

### 3. Temporal Engine
* **Role:** Complex Event Processing (CEP) and predictive modeling.
* **Core Components:** [temporal/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/temporal)
* **Mechanism:** Analyzes sequences of events over sliding windows. Predicts future failures based on event frequency trends and flags cascading alerts before they trigger critical alerts downstream.

### 4. World Model
* **Role:** Real-time topology awareness and situation assessment.
* **Core Components:** [world_model/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/world_model)
* **Mechanism:** Maintains lists of current active entities and their health rankings. It calculates a unified safety index, maintains active tasks assigned to executors, and flags overlapping operations.

### 5. Reasoning Engine
* **Role:** Evaluates anomalies and determines the best remediation.
* **Core Components:** [reasoning/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/reasoning)
* **Mechanism:** Follows a strict 8-step decision process:
  1. **Fast-Path Check:** Bypasses LLMs by evaluating heuristic rules in [fast_path.py](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/reasoning/fast_path.py) utilizing procedural playbooks.
  2. **Slow-Path LLM:** If no playbook fits, it queries the LLM (Claude-3.5-Sonnet / GPT-4) using a context prompt populated with the current situation, topological links from Neo4j, and past episodic experiences of successful/failed actions.
  3. **Action Verification:** Validates the recommended action against [actions.yaml](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/actions.yaml). Non-registered recommendations automatically escalate to humans.

### 6. Planning System
* **Role:** Translates high-level decisions into structured execution DAGs.
* **Core Components:** [planning/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/planning)
* **Mechanism:** Generates step-by-step plans mapping out target actions. It organizes execution steps based on dependency rules, registers validation steps, and injects **Approval Gates** (forcing a pause until an operator clicks approve for high-risk actions).

### 7. Execution Layer
* **Role:** Safely dispatches mitigation operations.
* **Core Components:** [execution/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/execution)
* **Mechanism:** Runs a plan's independent steps in parallel using `asyncio.gather`. Dispatches commands (like db query termination, rolling restarts, or consumer scaling) via specialized action handlers.

### 8. Feedback Loop
* **Role:** The self-correction and evaluation mechanism.
* **Core Components:** [feedback/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/feedback)
* **Mechanism:** Consumes `action_completed`/`action_failed` events. It waits for a configured delay, queries the World Model to check if the target resource returned to a healthy state, records the outcome, and **adjusts playbook confidence scores** in Procedural Memory.

### 9. Agent Coordinator
* **Role:** Spawns agents, monitors health, and resolves resource lock conflicts.
* **Core Components:** [coordinator/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/coordinator)
* **Mechanism:** Uses a graph-based **Conflict Resolver** to query Neo4j. If multiple planner agents attempt to perform operations on overlapping dependent systems (e.g. scaling down a database replica while another restarts the primary cluster), the coordinator blocks the lower-priority action.

### 10. Dashboard & Chaos Lab
* **Role:** Real-time state visualization and resilience validation.
* **Core Components:** [dashboard/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/dashboard), [chaos_lab/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/chaos_lab)
* **Mechanism:** Allows users to visualize active plans, topological graph structures, and trigger synthetic issues (such as injecting mock DB slow queries or synthetic Kafka lag) to watch Perceptron detect, reason, and heal the stack.

---

## 🔄 E2E Lifecycle Example: Self-Healing in Action

To understand how these systems work in harmony, let's look at what happens when a database query hogs primary connections:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│  1. SENSE: DatabaseAdapter polls DB cluster pg_stat_activity, notices a slow query (>10s).   │
│     It emits "db_slow_query" event to Kafka topic 'cognitive.events'.                        │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  2. REMEMBER: MemoryRouter consumes the event, routes to TimescaleDB (episodic log) and      │
│     Redis (active state). Neo4j maps slow query to "svc:postgres" and dependent applications.│
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  3. ASSESS: World Model updates situation index. MonitorAgent sees critical anomaly and      │
│     delegates task to PlannerAgent by publishing 'task_delegated' event to Kafka.            │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  4. DECIDE: PlannerAgent catches the task, calls Reasoning Engine. Reasoning checks rule      │
│     engine playbooks. None exists. It triggers slow-path: builds prompt with Neo4j           │
│     topological connections and pulls historic episodes of similar slow query incidents.     │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  5. PLAN: LLM suggests 'kill_slow_query' with target PID. Planning API drafts Plan DAG.     │
│     Because database query killing is marked 'risk: medium', it bypasses approval gates.     │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  6. EXECUTE: ExecutorAgent polls task from World Model, gets the plan, calls Execution       │
│     API, which runs PG pg_terminate_backend(pid). It publishes 'task_completed' event.       │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  7. EVALUATE: Feedback Loop catches 'task_completed'. It waits 45 seconds (settling window), │
│     queries World Model for DB health. DB query count is normal. Health is restored.         │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│  8. LEARN: Feedback updates graph weights and records success. A new high-confidence        │
│     playbook is stored in Procedural Memory so future occurrences hit the ms Fast-Path.      │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Getting Started & Local Execution

### 📋 Prerequisites
* **Python 3.10+**
* **Docker & Docker Compose**

### 1. Initialize External Target Network
The docker network must be created before launching containers, as it bridges with the target target systems:
```bash
docker network create target_system_shopcore
```

### 2. Start Core Infrastructure Containers
Start Redpanda, Redis, TimescaleDB, Neo4j Community, Qdrant, Prometheus, Alertmanager, and MQTT:
```bash
docker compose up -d
```
You can inspect active datastores/web interfaces:
* **Redpanda Console:** [http://localhost:8888](http://localhost:8888)
* **Neo4j Console:** [http://localhost:7474](http://localhost:7474) (Username: `neo4j`, Password: `password123`)
* **Qdrant DB:** [http://localhost:6333/dashboard](http://localhost:6333/dashboard)
* **Prometheus:** [http://localhost:9090](http://localhost:9090)

### 3. Run Schema Migrations
Initialize PostgreSQL tables, TimescaleDB hyper-tables, and verify databases:
```bash
python3 -m migrations.apply
```

### 4. Start all Cognitive Services
Start all 10 services in the background using the wrapper script:
```bash
./start_services.sh
```
This launches uvicorn servers for each layer on their respective ports:
* **Perception API:** Port `8080`
* **Memory API:** Port `8090`
* **Temporal Engine:** Port `8091`
* **World Model:** Port `8092`
* **Reasoning Engine:** Port `8093`
* **Planning API:** Port `8094`
* **Execution API:** Port `8095`
* **Feedback API:** Port `8096`
* **Coordinator API:** Port `8097`
* **Dashboard API:** Port `8000`

Logs for each component are written to local logs (e.g., `perception_orchestrator.log`, `memory_router.log`, `coordinator_api.log`).

### 5. Running Tests
Run full test coverage checks:
```bash
pytest
```

---

## 🗺️ Directory Map

* [cognitive_perception/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/cognitive_perception) — Normalizers, adapters, Pydantic schemas.
* [memory/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/memory) — Memory routing consumer and tier adapters (Working, Episodic, Semantic, Procedural, Vector).
* [temporal/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/temporal) — Time-series sequence analysis, trend/anomaly predictions.
* [world_model/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/world_model) — Entity registers, situation scoring, causal relations.
* [reasoning/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/reasoning) — Rule engines, LLM client connections, prompt template builders.
* [planning/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/planning) — DAG plan generators, execution templates, validation steps.
* [execution/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/execution) — Concurrent execution engines, task runners, shell/database/API action handlers.
* [feedback/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/feedback) — Post-action verifiers, playbook confidence modifiers.
* [coordinator/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/coordinator) — Agent fleet managers, conflict detection loops.
* [dashboard/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/dashboard) — Visual consoles and administrative user interfaces.
* [chaos_lab/](file:///Users/devanshkewlani/iCloud%20Drive%20%28Archive%29/Desktop/PROJECTS/perceptron/chaos_lab) — Incident injectors and network stress/lag testing suites.
