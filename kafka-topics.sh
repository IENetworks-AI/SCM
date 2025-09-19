#!/usr/bin/env bash
set -e

# find kafka container name (robust)
KAFKA_CONTAINER=$(docker ps --format '{{.Names}} {{.Image}}' | grep -E 'kafka|bitnami/kafka' | awk '{print $1}' | head -n1 || true)
if [ -z "$KAFKA_CONTAINER" ]; then
  echo "Kafka container not found. Aborting topic creation."
  exit 1
fi
echo "Using Kafka container: $KAFKA_CONTAINER"

# helper to run kafka-topics via the container
_run_kafka() {
  docker exec -i "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

# topics to ensure exist
_run_kafka --create --if-not-exists --topic scm_requests --partitions 3 --replication-factor 1
_run_kafka --create --if-not-exists --topic scm_inventory --partitions 3 --replication-factor 1
_run_kafka --create --if-not-exists --topic scm_predictions --partitions 3 --replication-factor 1

echo "Kafka topics ensured."
