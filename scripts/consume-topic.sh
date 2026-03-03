#!/usr/bin/env bash
# consume-topic.sh
# Tail messages from a Kafka topic using the kafka-console-consumer.
#
# Usage:
#   ./scripts/consume-topic.sh orders              (from newest)
#   ./scripts/consume-topic.sh orders --from-beginning
#   TOPIC=orders.dlq ./scripts/consume-topic.sh

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
TOPIC="${1:-orders}"
FROM_BEGINNING="${2:-}"

echo "Consuming from topic: ${TOPIC}"
echo "Broker: ${BROKER}"
echo "Press Ctrl+C to stop"
echo "──────────────────────────────────────────────────────────"

CMD=(
  docker exec -it kafka
  kafka-console-consumer
  --bootstrap-server "${BROKER}"
  --topic "${TOPIC}"
  --property print.key=true
  --property print.timestamp=true
  --property key.separator=" | "
  --property print.partition=true
  --property print.offset=true
)

if [[ "${FROM_BEGINNING}" == "--from-beginning" ]]; then
  CMD+=(--from-beginning)
fi

"${CMD[@]}"
