#!/bin/bash
set -e

echo "=== Step 1: Injecting consumer lag critical event into Perception API (8080) ==="
curl -s -X POST http://localhost:8080/perception/prometheus-alerts \
  -H "Content-Type: application/json" \
  -d "{\"alerts\":[{\"status\":\"firing\",
    \"labels\":{\"alertname\":\"KafkaConsumerLagHigh\",\"job\":\"order-processor\"},
    \"annotations\":{\"value\":\"18000\"},
    \"startsAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }]}"

echo "Waiting 5 seconds for Kafka events to propagate and World Model to update..."
sleep 5

echo "=== Step 2: Triggering Reasoning Engine (8093) ==="
DECISION=$(curl -s -X POST http://localhost:8093/reasoning/reason)
RECOMMENDED_ACTION=$(echo $DECISION | jq -r '.recommended_action')
echo "Decision recommended: $RECOMMENDED_ACTION"

echo "=== Step 3: Generating Plan from Decision (8094) ==="
PLAN=$(curl -s -X POST http://localhost:8094/planning/generate \
  -H "Content-Type: application/json" \
  -d "$DECISION")
PLAN_ID=$(echo $PLAN | jq -r '.plan_id')
echo "Plan Generated ID: $PLAN_ID"
echo $PLAN | jq '{plan_id: .plan_id, goal: .goal, steps: [.steps[] | {id: .step_id, action: .action, description: .description}]}'

echo "=== Step 4: Executing the Plan via Execution Layer (8095) ==="
EXEC_RESP=$(curl -s -X POST http://localhost:8095/execution/execute \
  -H "Content-Type: application/json" \
  -d "$PLAN")
echo "Execution response: $EXEC_RESP"

echo "Waiting 6 seconds for actions and checks to complete..."
sleep 6

echo "=== Step 5: Checking Plan Status in Planning Store (8094) ==="
curl -s "http://localhost:8094/planning/plans/$PLAN_ID" | jq '{status: .status, steps: [.steps[] | {id: .step_id, status: .status}]}'

echo "=== Step 6: Verifying open anomalies count in World Model (8092) ==="
ANOMALIES_COUNT=$(curl -s "http://localhost:8092/world/anomalies" | jq 'length')
echo "Current open anomalies count: $ANOMALIES_COUNT"
