#!/usr/bin/env sh
set -e

DATASET_ROOT="${DATASET_ROOT:-/Users/lolitha/Documents/MSC/BigData/Assignment/Final/Task1/data}"

resolve_script_directory() {
  CDPATH= cd -- "$(dirname -- "$0")" && pwd
}

ensure_spark_jars_copied() {
  if [ ! -d "spark-jars" ]; then
    echo "Copying Spark jars from spark-master ..."
    docker cp spark-master:/opt/spark/jars ./spark-jars
  fi
}

build_spark_classpath() {
  find spark-jars -name '*.jar' | tr '\n' ':'
}

compile_spark_java() {
  spark_cp="$1"
  echo "Compiling InDegreeSpark.java with classpath:"
  echo "$spark_cp"

  if javac --release 8 -cp "$spark_cp" InDegreeSpark.java; then
    echo "Compiled with --release 8"
  else
    echo "--release 8 not supported, falling back to -source/-target 1.8"
    javac -source 1.8 -target 1.8 -cp "$spark_cp" InDegreeSpark.java
  fi
}

create_spark_jar() {
  jar_name="$1"
  echo "Creating Spark JAR: $jar_name"
  rm -f "$jar_name"
  jar cf "$jar_name" InDegreeSpark.class
}

copy_spark_jar_to_master() {
  jar_name="$1"
  echo "Copying Spark JAR to spark-master ..."
  docker exec spark-master mkdir -p /tmp/indegree
  docker cp "$jar_name" "spark-master:/tmp/indegree/$jar_name"
}

ensure_spark_runtime_csv_header() {
  runtime_csv="$1"
  if [ ! -f "$runtime_csv" ]; then
    echo "dataset,start_ts,end_ts,duration_sec" > "$runtime_csv"
  fi
}

ensure_spark_stats_csv_header() {
  stats_csv="$1"
  if [ ! -f "$stats_csv" ]; then
    echo "dataset,timestamp,container,cpu,mem,mem_pct,net_io,blk_io,pids" > "$stats_csv"
  fi
}

record_spark_stats_snapshot() {
  dataset_name="$1"
  stats_csv="$2"
  timestamp="$(date +%s)"

  docker stats --no-stream spark-master spark-worker \
    --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' \
  | while IFS= read -r line; do
      echo "${dataset_name},${timestamp},${line}" >> "$stats_csv"
    done
}

run_spark_stats_loop_for_dataset() {
  dataset_name="$1"
  stats_csv="$2"
  interval_seconds="$3"

  ensure_spark_stats_csv_header "$stats_csv"

  while :; do
    record_spark_stats_snapshot "$dataset_name" "$stats_csv"
    sleep "$interval_seconds"
  done
}

discover_spark_datasets() {
  found_any=0
  for f in "${DATASET_ROOT}"/*.txt; do
    if [ -f "$f" ]; then
      found_any=1
      base="$(basename "$f")"
      dataset_name="${base%.txt}"
      echo "$dataset_name"
    fi
  done

  if [ "$found_any" -eq 0 ]; then
    echo "ERROR: No *.txt datasets found in DATASET_ROOT=${DATASET_ROOT}" >&2
    exit 1
  fi
}

run_spark_dataset_with_metrics() {
  dataset_name="$1"
  jar_name="$2"
  runtime_csv="$3"

  stats_csv="spark_stats_${dataset_name}.csv"
  echo "Starting docker stats collection for ${dataset_name} (Spark) ..."
  run_spark_stats_loop_for_dataset "$dataset_name" "$stats_csv" 5 &
  stats_pid=$!

  start_timestamp="$(date +%s)"
  echo "Running Spark jobs for dataset ${dataset_name} ..."
  docker exec spark-master /opt/spark/bin/spark-submit \
    --class InDegreeSpark \
    --master spark://spark-master:7077 \
    --conf spark.default.parallelism=8 \
    "/tmp/indegree/${jar_name}" \
    "${dataset_name}"
  end_timestamp="$(date +%s)"
  duration_seconds=$((end_timestamp - start_timestamp))

  echo "${dataset_name},${start_timestamp},${end_timestamp},${duration_seconds}" >> "$runtime_csv"
  echo "Spark dataset ${dataset_name} completed in ${duration_seconds} seconds"

  kill "$stats_pid" 2>/dev/null || true
}

run_all_spark_datasets() {
  jar_name="$1"
  runtime_csv="$2"

  DATASET_LIST="$(discover_spark_datasets)"

  echo "Discovered Spark datasets:"
  echo "$DATASET_LIST"

  for dataset_name in $DATASET_LIST; do
    run_spark_dataset_with_metrics "$dataset_name" "$jar_name" "$runtime_csv"
  done
}

SCRIPT_DIR="$(resolve_script_directory)"
cd "$SCRIPT_DIR"

SPARK_JAR_NAME="spark-indegree.jar"
SPARK_RUNTIME_CSV_FILE="spark_runtime_per_dataset.csv"

ensure_spark_jars_copied
SPARK_CP="$(build_spark_classpath)"
compile_spark_java "$SPARK_CP"
create_spark_jar "$SPARK_JAR_NAME"
copy_spark_jar_to_master "$SPARK_JAR_NAME"
ensure_spark_runtime_csv_header "$SPARK_RUNTIME_CSV_FILE"
run_all_spark_datasets "$SPARK_JAR_NAME" "$SPARK_RUNTIME_CSV_FILE"
