#!/bin/bash
echo "Stopping Cognitive Architecture services started by start_services.sh..."

PID_FILE=".dca_services.pids"

if [ -f "$PID_FILE" ]; then
  while read -r pid; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      echo "Stopped pid $pid"
    fi
  done < "$PID_FILE"
  rm -f "$PID_FILE"
else
  echo "No $PID_FILE found. Nothing to stop."
fi
