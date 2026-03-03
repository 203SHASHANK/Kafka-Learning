// Package metrics provides Prometheus instrumentation for Kafka operations.
//
// WHY instrument Kafka?
// Without metrics, you cannot answer these critical operational questions:
//   - Is my producer keeping up? (messages_produced_total rate)
//   - Are consumers falling behind? (consumer_lag gauge)
//   - How many messages are failing? (consume_errors_total)
//   - What's my end-to-end processing latency? (processing_duration_seconds)
//
// Consumer lag is the single most important Kafka metric.
// It tells you how far behind your consumers are from the producer.
// Rising lag = consumers can't keep up → scale out or optimize the handler.
//
// Expose metrics: run your app and visit http://localhost:2112/metrics
// Then scrape with Prometheus and visualize in Grafana.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Producer Metrics ────────────────────────────────────────────────────────

var (
	// MessagesProduced counts successfully produced messages by topic.
	// Alert if this rate drops to zero when you expect production traffic.
	MessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_produced_total",
			Help: "Total number of messages successfully produced to Kafka, by topic",
		},
		[]string{"topic"},
	)

	// ProduceErrors counts produce failures by topic.
	// Alert if this is non-zero—every error is a potential message loss.
	ProduceErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_produce_errors_total",
			Help: "Total number of produce failures, by topic",
		},
		[]string{"topic"},
	)

	// ProduceDuration measures produce latency (how long SendMessage blocks).
	// P99 > 100ms usually indicates broker or network problems.
	ProduceDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_produce_duration_seconds",
			Help:    "Time taken to produce a message (sync) to Kafka",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
		[]string{"topic"},
	)
)

// ─── Consumer Metrics ────────────────────────────────────────────────────────

var (
	// MessagesConsumed counts messages consumed per topic/group.
	MessagesConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_consumed_total",
			Help: "Total number of messages consumed from Kafka",
		},
		[]string{"topic", "consumer_group"},
	)

	// ConsumeErrors counts messages that failed processing.
	ConsumeErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consume_errors_total",
			Help: "Total number of message processing failures",
		},
		[]string{"topic", "consumer_group"},
	)

	// ProcessingDuration measures how long your message handler takes.
	// High values indicate slow handler code—optimize or scale horizontally.
	// Buckets go up to 10s to capture slow database operations.
	ProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_message_processing_duration_seconds",
			Help:    "Time taken to process a single Kafka message",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"topic", "consumer_group"},
	)

	// ConsumerLag is the current offset lag per topic/partition/group.
	// THE most important Kafka metric. Rising lag = production problem.
	// Alert when lag exceeds your SLA tolerance (e.g., >10,000 messages).
	ConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current consumer lag (messages behind latest) per partition",
		},
		[]string{"topic", "partition", "consumer_group"},
	)

	// Rebalances counts how often the consumer group rebalances.
	// Occasional rebalances are normal (deploy, scaling). Frequent rebalances
	// indicate consumers crashing or processing too slowly (session timeout).
	Rebalances = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_rebalances_total",
			Help: "Total number of consumer group rebalances",
		},
		[]string{"consumer_group"},
	)
)

// ─── DLQ Metrics ─────────────────────────────────────────────────────────────

var (
	// DLQMessages counts messages sent to the dead letter queue.
	// Alert if this is non-zero—DLQ messages need human investigation.
	DLQMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_dlq_messages_total",
			Help: "Total number of messages forwarded to the dead letter queue",
		},
		[]string{"source_topic", "dlq_topic", "reason"},
	)

	// RetryAttempts tracks retry counts by topic.
	RetryAttempts = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_retry_attempts",
			Help:    "Number of retry attempts before success or DLQ",
			Buckets: []float64{1, 2, 3, 4, 5},
		},
		[]string{"topic"},
	)
)

// ServeHTTP starts a Prometheus metrics HTTP server on the given address.
// Typically called in a goroutine: go metrics.ServeHTTP(":2112")
func ServeHTTP(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return http.ListenAndServe(addr, mux)
}
