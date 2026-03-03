#!/usr/bin/env bash
# list-topics.sh
# Lists all topics with their partition and replication details.

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"

echo "Kafka Topics on ${BROKER}"
echo "════════════════════════════════════════"
echo ""

# List all topics with full description
docker exec kafka kafka-topics \
  --bootstrap-server "${BROKER}" \
  --describe

echo ""
echo "Consumer Groups:"
echo "════════════════════════════════════════"
docker exec kafka kafka-consumer-groups \
  --bootstrap-server "${BROKER}" \
  --list 2>/dev/null || echo "(no consumer groups found)"
