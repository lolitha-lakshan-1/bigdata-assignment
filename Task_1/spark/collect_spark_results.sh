#!/usr/bin/env sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULT_DIR="$SCRIPT_DIR/result-spark"

mkdir -p "$RESULT_DIR"

echo "Copying Spark output CSVs from spark-master:/tmp/indegree ..."

remote_files=$(docker exec spark-master sh -lc 'ls /tmp/indegree/*-indegree-spark.csv 2>/dev/null || true')

if [ -z "$remote_files" ]; then
  echo "No Spark output files found in /tmp/indegree matching *-indegree-spark.csv"
else
  for remote_path in $remote_files; do
    base=$(basename "$remote_path")
    if docker cp "spark-master:${remote_path}" "$RESULT_DIR/${base}"; then
      echo "Copied Spark output file ${base}"
    else
      echo "Warning: failed to copy ${remote_path}"
    fi
  done
fi

echo "Copying Spark stats from host ..."

for f in "$SCRIPT_DIR"/spark_stats_*.csv; do
  if [ -f "$f" ]; then
    cp "$f" "$RESULT_DIR/"
    echo "Copied Spark stats file $(basename "$f")"
  fi
done

if [ -f "$SCRIPT_DIR/spark_runtime_per_dataset.csv" ]; then
  cp "$SCRIPT_DIR/spark_runtime_per_dataset.csv" "$RESULT_DIR/"
  echo "Copied spark_runtime_per_dataset.csv"
fi

echo "Done. Contents of $RESULT_DIR:"
ls -1 "$RESULT_DIR"
