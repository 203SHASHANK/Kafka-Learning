#!/usr/bin/env bash
# create-topics.sh
# Creates all Kafka topics required for the examples.
#
# Run after starting Kafka: make docker-up && make create-topics
#
# WHY create topics explicitly?
# Kafka can auto-create topics (KAFKA_AUTO_CREATE_TOPICS_ENABLE=true in dev),
# but in production you want explicit control over:
#   - Number of partitions (determines max parallelism)
#   - Replication factor (determines fault tolerance)
#   - Retention settings (how long messages are kept)
#   - Cleanup policy (delete vs compact)

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
REPLICATION="${REPLICATION_FACTOR:-1}"  # 1 for single-node, 3 for multi-node

echo "Creating Kafka topics on broker: ${BROKER}"
echo "Replication factor: ${REPLICATION}"
echo ""

# Helper to create a topic, ignoring "already exists" errors.
create_topic() {
  local name="$1"
  local partitions="$2"
  local extra_config="${3:-}"

  echo -n "  Creating topic '${name}' (partitions=${partitions})... "

  local cmd=(
    docker exec kafka
    kafka-topics
    --bootstrap-server "${BROKER}"
    --create
    --if-not-exists
    --topic "${name}"
    --partitions "${partitions}"
    --replication-factor "${REPLICATION}"
  )

  if [[ -n "${extra_config}" ]]; then
    cmd+=(--config "${extra_config}")
  fi

  if "${cmd[@]}" 2>&1 | grep -qE "(Created topic|already exists)"; then
    echo "OK"
  else
    "${cmd[@]}" && echo "OK" || echo "FAILED (topic may already exist, continuing)"
  fi
}

echo "── Basic Topics ──────────────────────────────────────────────────────────"
create_topic "orders"        6                              # Main orders topic, 6 partitions for parallelism
create_topic "orders.dlq"    3                              # Dead letter queue
create_topic "payments"      3                              # Payment events
create_topic "notifications" 3                              # Notification events
create_topic "enriched-orders" 6                           # Output for exactly-once example
create_topic "high-throughput-demo" 6                      # Benchmark topic

echo ""
echo "── Compacted Topics (for changelog/CDC patterns) ─────────────────────────"
create_topic "user-preferences" 3 "cleanup.policy=compact" # Keep only latest value per key

echo ""
echo "── Listing all topics ────────────────────────────────────────────────────"
docker exec kafka kafka-topics \
  --bootstrap-server "${BROKER}" \
  --list

echo ""
echo "Done. All topics created."
