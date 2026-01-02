#!/usr/bin/env sh
set -e

DATASET_ROOT="${DATASET_ROOT:-/Users/lolitha/Documents/MSC/BigData/Assignment/Final2/Task_1/data}"

resolve_script_directory() {
  CDPATH= cd -- "$(dirname -- "$0")" && pwd
}

ensure_hadoop_share_copied() {
  if [ ! -d "hadoop-share" ]; then
    echo "Copying Hadoop jars from namenode ..."
    docker cp namenode:/opt/hadoop/share ./hadoop-share
  fi
}

build_hadoop_classpath() {
  find hadoop-share -name '*.jar' | tr '\n' ':'
}

compile_java_sources() {
  hadoop_cp="$1"
  echo "Compiling Java sources with classpath:"
  echo "$hadoop_cp"

  if javac --release 8 -cp "$hadoop_cp" InDegreeJob1Hadoop.java InDegreeJob2Hadoop.java InDegreeMainHadoop.java; then
    echo "Compiled with --release 8"
  else
    echo "--release 8 not supported, falling back to -source/-target 1.8"
    javac -source 1.8 -target 1.8 -cp "$hadoop_cp" InDegreeJob1Hadoop.java InDegreeJob2Hadoop.java InDegreeMainHadoop.java
  fi
}

create_jar() {
  jar_name="$1"
  echo "Creating JAR: $jar_name"
  rm -f "$jar_name"
  jar cf "$jar_name" *.class
}

copy_jar_to_namenode() {
  jar_name="$1"
  echo "Copying JAR to namenode ..."
  docker exec namenode mkdir -p /opt/indegree
  docker cp "$jar_name" "namenode:/opt/indegree/$jar_name"
}

ensure_runtime_csv_header() {
  runtime_csv="$1"
  if [ ! -f "$runtime_csv" ]; then
    echo "dataset,start_ts,end_ts,duration_sec" > "$runtime_csv"
  fi
}

ensure_stats_csv_header() {
  stats_csv="$1"
  if [ ! -f "$stats_csv" ]; then
    echo "dataset,timestamp,container,cpu,mem,mem_pct,net_io,blk_io,pids" > "$stats_csv"
  fi
}

record_stats_snapshot() {
  dataset_name="$1"
  stats_csv="$2"
  timestamp="$(date +%s)"

  docker stats --no-stream namenode datanode resourcemanager nodemanager     --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}'   | while IFS= read -r line; do
      echo "${dataset_name},${timestamp},${line}" >> "$stats_csv"
    done
}

run_stats_loop_for_dataset() {
  dataset_name="$1"
  stats_csv="$2"
  interval_seconds="$3"

  ensure_stats_csv_header "$stats_csv"

  while :; do
    record_stats_snapshot "$dataset_name" "$stats_csv"
    sleep "$interval_seconds"
  done
}

stage_dataset_to_hdfs() {
  dataset_name="$1"

  local_file="${dataset_name}.txt"
  local_dataset_path="${DATASET_ROOT}/${local_file}"

  if [ ! -f "$local_dataset_path" ]; then
    echo "ERROR: Local dataset file not found: $local_dataset_path" >&2
    exit 1
  fi

  echo "Staging dataset ${dataset_name} from ${local_dataset_path} into namenode and HDFS ..."
  docker exec namenode mkdir -p /data
  docker cp "$local_dataset_path" "namenode:/data/${dataset_name}-hadoop.txt"

  docker exec namenode hdfs dfs -mkdir -p /input
  docker exec namenode hdfs dfs -rm -f "/input/${dataset_name}-hadoop.txt" >/dev/null 2>&1 || true
  docker exec namenode hdfs dfs -put "/data/${dataset_name}-hadoop.txt" "/input/${dataset_name}-hadoop.txt"
}

run_dataset_with_metrics() {
  dataset_name="$1"
  jar_name="$2"
  runtime_csv="$3"

  stage_dataset_to_hdfs "$dataset_name"

  stats_csv="hadoop_stats_${dataset_name}.csv"
  echo "Starting docker stats collection for ${dataset_name} ..."
  run_stats_loop_for_dataset "$dataset_name" "$stats_csv" 5 &
  stats_pid=$!

  start_timestamp="$(date +%s)"
  echo "Running Hadoop jobs for dataset ${dataset_name} ..."
  docker exec namenode sh -lc "hadoop jar /opt/indegree/${jar_name} InDegreeMainHadoop ${dataset_name}"
  end_timestamp="$(date +%s)"
  duration_seconds=$((end_timestamp - start_timestamp))

  echo "${dataset_name},${start_timestamp},${end_timestamp},${duration_seconds}" >> "$runtime_csv"
  echo "Dataset ${dataset_name} completed in ${duration_seconds} seconds"

  kill "$stats_pid" 2>/dev/null || true
}

discover_datasets() {
  # Find all *.txt under DATASET_ROOT and turn them into dataset names
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

run_all_datasets() {
  jar_name="$1"
  runtime_csv="$2"

  DATASET_LIST="$(discover_datasets)"

  echo "Discovered datasets:"
  echo "$DATASET_LIST"

  for dataset_name in $DATASET_LIST; do
    run_dataset_with_metrics "$dataset_name" "$jar_name" "$runtime_csv"
  done
}

SCRIPT_DIR="$(resolve_script_directory)"
cd "$SCRIPT_DIR"

# Root folder where plain-text datasets live on host.
# Default: "<project>/data", can override via environment: DATASET_ROOT=/some/path ./run_hadoop_indegree.sh

JAR_NAME="indegree-hadoop.jar"
RUNTIME_CSV_FILE="hadoop_runtime_per_dataset.csv"

ensure_hadoop_share_copied
HADOOP_CP="$(build_hadoop_classpath)"
compile_java_sources "$HADOOP_CP"
create_jar "$JAR_NAME"
copy_jar_to_namenode "$JAR_NAME"
ensure_runtime_csv_header "$RUNTIME_CSV_FILE"
run_all_datasets "$JAR_NAME" "$RUNTIME_CSV_FILE"
