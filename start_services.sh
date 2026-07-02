#!/bin/bash
set -e

echo "Starting all Cognitive Architecture API services..."

PID_FILE=".dca_services.pids"
: > "$PID_FILE"

start_bg() {
  local name="$1"
  local logfile="$2"
  shift 2
  nohup "$@" > "$logfile" 2>&1 &
  local pid=$!
  echo "$pid" >> "$PID_FILE"
  echo "Started $name (pid $pid, log $logfile)"
}

if [ "$1" = "--fresh" ]; then
  echo "Resetting DCA databases because --fresh was provided..."
  python3 reset_dca.py
fi

echo "Applying database migrations if backing stores are available..."
python3 -m migrations.apply || true

if [ -x "./infrastructure/init_kafka_topics.sh" ]; then
  echo "Ensuring DCA Kafka topics (raw.logs, cognitive.events)..."
  ./infrastructure/init_kafka_topics.sh || true
fi

start_bg "Perception API on port 8080" perception_api.log python3 -m uvicorn cognitive_perception.perception_api:app --port 8080
start_bg "Memory API on port 8090" memory_api.log python3 -u -m uvicorn memory.api:app --port 8090
start_bg "Memory Router consumer" memory_router.log python3 -m memory.consumer
start_bg "Temporal Engine API on port 8091" temporal_api.log python3 -m uvicorn temporal.api:app --port 8091
start_bg "Temporal Ingester consumer" temporal_ingester.log python3 -m temporal.ingester
start_bg "World Model API on port 8092" world_model_api.log python3 -m uvicorn world_model.api:app --port 8092
start_bg "Reasoning Engine API on port 8093" reasoning_api.log python3 -m uvicorn reasoning.api:app --port 8093
start_bg "Planning System API on port 8094" planning_api.log python3 -m uvicorn planning.api:app --port 8094
start_bg "Execution Layer API on port 8095" execution_api.log python3 -m uvicorn execution.api:app --port 8095
start_bg "Feedback Loop API on port 8096" feedback_api.log python3 -m uvicorn feedback.api:app --port 8096
start_bg "Agent Coordinator API on port 8097" coordinator_api.log python3 -m uvicorn coordinator.api:app --port 8097
start_bg "Dashboard API on port 8000" dashboard_api.log python3 -m uvicorn dashboard.api:app --port 8000
start_bg "Perception Orchestrator adapters" perception_orchestrator.log python3 -u perception_main.py

sleep 5
echo "All services are starting in the background. PIDs saved in $PID_FILE. Check *.log files for details."
