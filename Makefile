.PHONY: all build test lint clean deps \
        docker-up docker-down docker-up-multi docker-down-multi \
        create-topics \
        run-producer run-consumer run-consumer-groups run-offset run-keys \
        run-serialization run-dlq run-idempotent run-graceful \
        run-exactly-once run-transactions run-high-throughput help

BINARY_DIR := bin
GOOS       ?= $(shell go env GOOS)
GOARCH     ?= $(shell go env GOARCH)

# ─── Build ─────────────────────────────────────────────────────────────────────

all: build

## Build all binaries under cmd/
build:
	@mkdir -p $(BINARY_DIR)
	@for dir in cmd/*/; do \
		name=$$(basename $$dir); \
		echo "  → Building $$name"; \
		go build -ldflags="-s -w" -o $(BINARY_DIR)/$$name ./$$dir; \
	done
	@echo "Build complete. Binaries in $(BINARY_DIR)/"

# ─── Test ──────────────────────────────────────────────────────────────────────

## Run all tests with race detector
test:
	go test -v -race ./...

## Run tests and generate HTML coverage report
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# ─── Code Quality ──────────────────────────────────────────────────────────────

## Run golangci-lint (install: https://golangci-lint.run/usage/install/)
lint:
	golangci-lint run ./...

## Format code
fmt:
	gofmt -w .

## Tidy module dependencies
deps:
	go mod download
	go mod tidy

# ─── Docker ────────────────────────────────────────────────────────────────────

## Start single-node Kafka (KRaft mode, no Zookeeper)
docker-up:
	docker compose -f docker/single-node/docker-compose.yml up -d
	@echo ""
	@echo "Kafka is starting... Waiting for health check."
	@echo "  Kafka broker:  localhost:9092"
	@echo "  Kafka UI:      http://localhost:8080"
	@echo ""

## Stop single-node Kafka and remove volumes
docker-down:
	docker compose -f docker/single-node/docker-compose.yml down -v

## Start 3-broker Kafka cluster
docker-up-multi:
	docker compose -f docker/multi-node/docker-compose.yml up -d
	@echo ""
	@echo "3-broker Kafka cluster starting..."
	@echo "  Brokers: localhost:9092, localhost:9093, localhost:9094"
	@echo "  Kafka UI: http://localhost:8080"
	@echo ""

## Stop multi-node Kafka
docker-down-multi:
	docker compose -f docker/multi-node/docker-compose.yml down -v

## Start Prometheus + Grafana monitoring stack
docker-monitoring:
	docker compose -f docker/monitoring/docker-compose.yml up -d
	@echo ""
	@echo "Monitoring stack starting..."
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"
	@echo ""

# ─── Kafka Admin ───────────────────────────────────────────────────────────────

## Create all required topics
create-topics:
	@chmod +x scripts/create-topics.sh
	./scripts/create-topics.sh

## List all topics
list-topics:
	@chmod +x scripts/list-topics.sh
	./scripts/list-topics.sh

## Run health check against Kafka
health-check:
	@chmod +x scripts/health-check.sh
	./scripts/health-check.sh

# ─── Examples ──────────────────────────────────────────────────────────────────

## [01] Basic sync producer
run-producer:
	go run examples/01-basics/producer/main.go

## [01] Basic consumer (standalone partition consumer)
run-consumer:
	go run examples/01-basics/consumer/main.go

## [02] Consumer groups with partition rebalancing
run-consumer-groups:
	go run examples/02-consumer-groups/main.go

## [03] Manual offset management and seeking
run-offset:
	go run examples/03-offset-management/main.go

## [04] Message keys and partition routing
run-keys:
	go run examples/04-message-keys/main.go

## [05] JSON serialization with typed messages
run-serialization:
	go run examples/05-serialization/main.go

## [06] Retry logic and dead letter queue pattern
run-dlq:
	go run examples/06-retry-dlq/main.go

## [07] Idempotent producer (no duplicate messages on retry)
run-idempotent:
	go run examples/07-idempotent-producer/main.go

## [08] Graceful shutdown with signal handling
run-graceful:
	go run examples/08-graceful-shutdown/main.go

## [09] Exactly-once semantics (read-process-write)
run-exactly-once:
	go run examples/09-exactly-once/main.go

## [10] Kafka transactions (atomic multi-topic writes)
run-transactions:
	go run examples/10-transactions/main.go

## [11] High-throughput async producer tuning
run-high-throughput:
	go run examples/11-high-throughput/main.go

# ─── Utilities ─────────────────────────────────────────────────────────────────

## Remove build artifacts
clean:
	rm -rf $(BINARY_DIR) coverage.out coverage.html

## Show all available targets
help:
	@echo ""
	@echo "Kafka Learning - Available Make Targets"
	@echo "======================================="
	@grep -E '^## ' Makefile | sed 's/## /  /'
	@echo ""
