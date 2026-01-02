Read Me
=======

Run Hadoop
==========

Prerequisites
  - hadoop_config folder
  - stop all other containers to prevent OOM errors.
  - datasets in the data folder

docker-compose -f hadoop-cluster.yml up -d
docker compose -f hadoop-cluster.yml down


sh run_hadoop_indegree.sh

sh collect_stats.sh - to get the indegree results

 
 
