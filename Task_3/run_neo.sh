#!/usr/bin/env bash

set -e

IMAGE="neo4j:latest"
CONTAINER_NAME="neo4j-graphdb"

echo "Pulling image: $IMAGE ..."
docker pull "$IMAGE"

echo "Starting container: $CONTAINER_NAME ..."
docker run -d \
  --name "$CONTAINER_NAME" \
  -p7474:7474 -p7687:7687 \
  -e NEO4J_AUTH=neo4j/test12345 \
  "$IMAGE"

echo "Neo4j is starting!"
echo "Browser UI: http://localhost:7474"
echo "Bolt URL : bolt://localhost:7687"
echo "User/pass: neo4j / test12345"
