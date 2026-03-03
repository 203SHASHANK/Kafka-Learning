#!/usr/bin/env bash
# health-check.sh
# Verifies Kafka is running and responding to API requests.
#
# Returns exit code 0 if healthy, 1 if unhealthy.
# Useful in CI/CD pipelines and monitoring scripts.

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
MAX_RETRIES=10
RETRY_INTERVAL=3

echo "Kafka Health Check"
echo "Broker: ${BROKER}"
echo ""

check_kafka() {
  docker exec kafka kafka-broker-api-versions \
    --bootstrap-server "${BROKER}" \
    > /dev/null 2>&1
}

echo -n "Checking broker connectivity"
for i in $(seq 1 ${MAX_RETRIES}); do
  if check_kafka; then
    echo " ✓"
    break
  fi
  echo -n "."
  if [[ "${i}" == "${MAX_RETRIES}" ]]; then
    echo " ✗"
    echo "ERROR: Kafka not responding after ${MAX_RETRIES} attempts"
    echo "Make sure Kafka is running: make docker-up"
    exit 1
  fi
  sleep "${RETRY_INTERVAL}"
done

echo ""
echo "Broker API Versions:"
docker exec kafka kafka-broker-api-versions \
  --bootstrap-server "${BROKER}" 2>/dev/null | head -5

echo ""
echo "Topic Count:"
topic_count=$(docker exec kafka kafka-topics \
  --bootstrap-server "${BROKER}" \
  --list 2>/dev/null | wc -l | tr -d ' ')
echo "  ${topic_count} topics"

echo ""
echo "✓ Kafka is healthy and ready"
