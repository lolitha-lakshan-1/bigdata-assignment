#!/usr/bin/env bash

# Usage:
#   ./monitor_flink_with_docker.sh <TASKMANAGER_ID> <JOB_ID> <VERTEX_ID> <DOCKER_CONTAINER_NAME>
#
# Example:
#   ./monitor_flink_with_docker.sh \
#     172.22.0.5:36087-cd6c34 \
#     238a005fd5eb753db293a3d0debb58c2 \
#     cbc357ccb763df2852fee8c4fc7d55f2 \
#     flink-taskmanager-task4

JOBMANAGER_URL=${JOBMANAGER_URL:-http://localhost:8081}
TM_ID="$1"
JOB_ID="$2"
VERTEX_ID="$3"
CONTAINER_NAME="$4"

if [ -z "$TM_ID" ] || [ -z "$JOB_ID" ] || [ -z "$VERTEX_ID" ] || [ -z "$CONTAINER_NAME" ]; then
  echo "Usage: $0 <taskmanager-id> <job-id> <vertex-id> <docker-container-name>"
  exit 1
fi

echo "Monitoring TM=$TM_ID JOB=$JOB_ID VERTEX=$VERTEX_ID CONTAINER=$CONTAINER_NAME"
echo "Press Ctrl+C to stop."
echo ""
printf "%-25s | %8s | %10s | %10s | %14s | %14s | %12s | %22s\n" \
  "timestamp" "CPU(%)" "HeapUsedMB" "HeapMaxMB" "recordsOut(sum)" "rateOut(rec/s)" "DockCPU(%)" "DockMem(used/limit)"
echo "------------------------------------------------------------------------------------------------------------------------------------------"

while true; do
  ts=$(date -Ins)

  # ---------- Flink TaskManager JVM metrics (CPU + heap) ----------
  tm_json=$(curl -s "$JOBMANAGER_URL/taskmanagers/$TM_ID/metrics?get=Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max")

  cpu_load=$(echo "$tm_json"  | jq -r '.[] | select(.id=="Status.JVM.CPU.Load")         | .value')
  heap_used=$(echo "$tm_json" | jq -r '.[] | select(.id=="Status.JVM.Memory.Heap.Used") | .value')
  heap_max=$(echo "$tm_json"  | jq -r '.[] | select(.id=="Status.JVM.Memory.Heap.Max")  | .value')

  [ "$cpu_load" = "null" ] && cpu_load=0
  [ "$heap_used" = "null" ] && heap_used=0
  [ "$heap_max" = "null" ] && heap_max=1

  cpu_percent=$(awk "BEGIN {printf \"%.2f\", $cpu_load * 100}")
  heap_used_mb=$(awk "BEGIN {printf \"%.1f\", $heap_used / (1024*1024)}")
  heap_max_mb=$(awk "BEGIN {printf \"%.1f\", $heap_max / (1024*1024)}")

  # ---------- Flink job / vertex metrics (records OUT from this vertex) ----------
  job_json=$(curl -s "$JOBMANAGER_URL/jobs/$JOB_ID/vertices/$VERTEX_ID/subtasks/metrics?get=numRecordsOut,numRecordsOutPerSecond")

  records_out=$(echo "$job_json" | jq -r '.[] | select(.id=="numRecordsOut")           | .sum // 0')
  rate_out=$(echo "$job_json"    | jq -r '.[] | select(.id=="numRecordsOutPerSecond") | .max // 0')

  [ "$records_out" = "null" ] && records_out=0
  [ "$rate_out" = "null" ] && rate_out=0

  # ---------- Docker container stats (CPU + RAM for whole TM process) ----------
  docker_line=$(docker stats --no-stream --format "{{.CPUPerc}},{{.MemUsage}}" "$CONTAINER_NAME" 2>/dev/null)

  if [ -n "$docker_line" ]; then
    dock_cpu=$(echo "$docker_line" | cut -d',' -f1 | tr -d '%')
    mem_usage=$(echo "$docker_line" | cut -d',' -f2)          # e.g. "123.4MiB / 1.94GiB"
    dock_mem_used=$(echo "$mem_usage" | awk '{print $1}')     # "123.4MiB"
    dock_mem_limit=$(echo "$mem_usage" | awk '{print $3}')    # "1.94GiB"
    dock_mem="${dock_mem_used}/${dock_mem_limit}"
  else
    dock_cpu="0.0"
    dock_mem="N/A"
  fi

  printf "%-25s | %8s | %10s | %10s | %14s | %14s | %12s | %22s\n" \
    "$ts" "$cpu_percent" "$heap_used_mb" "$heap_max_mb" "$records_out" "$rate_out" "$dock_cpu" "$dock_mem"

  sleep 1
done
