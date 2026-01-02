Read Me
=======

Prerequisites
  - stop all other containers to prevent OOM errors.
  - datasets in the data folder
  - change volumes to data folder in the docker file 
    volumes:
      - /Users/lolitha/Documents/MSC/BigData/Assignment/Final/Task1/data:/data

docker compose -f spark-cluster.yml up -d
docker-compose -f spark-cluster.yml down


sh run_spark_indegree.sh
sh run_spark_indegree_optmized.sh
 

sh collect_stats.sh - to get the indegree results

 
 
