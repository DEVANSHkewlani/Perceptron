#!/bin/bash
# Ensure DCA Kafka topics exist for ShopCore log shipping and cognitive pipeline.
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=("raw.logs" "cognitive.events" "cognitive.perception_failures")

echo "Ensuring Kafka topics on ${BOOTSTRAP}..."

for topic in "${TOPICS[@]}"; do
  docker exec cognitive-redpanda rpk topic create "$topic" -p 1 -r 1 2>/dev/null \
    && echo "Created topic: $topic" \
    || echo "Topic exists or broker unavailable: $topic"
done
