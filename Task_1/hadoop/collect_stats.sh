#!/bin/sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
RESULT_DIR="$SCRIPT_DIR/result"

mkdir -p "$RESULT_DIR"

echo "Copying final CSV outputs from namenode:/data ..."

# Find all *-indegree-hadoop.csv files inside namenode:/data
remote_files=$(docker exec namenode sh -lc 'ls /data/*-indegree-hadoop.csv 2>/dev/null || true')

if [ -z "$remote_files" ]; then
  echo "No output files found in /data matching *-indegree-hadoop.csv"
else
  for remote_path in $remote_files; do
    base=$(basename "$remote_path")
    if docker cp "namenode:${remote_path}" "$RESULT_DIR/${base}"; then
      echo "Copied output file ${base}"
    else
      echo "Warning: failed to copy ${remote_path}"
    fi
  done
fi

echo "Copying stats from host ..."

# Copy all hadoop_stats_*.csv in the same folder as the script
for f in "$SCRIPT_DIR"/hadoop_stats_*.csv; do
  if [ -f "$f" ]; then
    cp "$f" "$RESULT_DIR/"
    echo "Copied stats file $(basename "$f")"
  fi
done

# Copy runtime summary if it exists
if [ -f "$SCRIPT_DIR/hadoop_runtime_per_dataset.csv" ]; then
  cp "$SCRIPT_DIR/hadoop_runtime_per_dataset.csv" "$RESULT_DIR/"
  echo "Copied hadoop_runtime_per_dataset.csv"
fi

echo "Done. Contents of $RESULT_DIR:"
ls -1 "$RESULT_DIR"
