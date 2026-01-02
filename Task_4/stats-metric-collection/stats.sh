#!/usr/bin/env bash

CONTAINER_NAME="flink-taskmanager-task4"
INTERVAL=1   # seconds

echo "timestamp,cpu_perc,mem_usage,mem_perc"

while true; do
  TS="$(date '+%Y-%m-%d %H:%M:%S')"
  LINE="$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' "$CONTAINER_NAME")"
  echo "$TS,$LINE"
  sleep "$INTERVAL"
done
