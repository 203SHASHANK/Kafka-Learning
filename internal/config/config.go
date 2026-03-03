// Package config handles all application configuration via environment variables.
//
// WHY environment variables?
// The 12-Factor App methodology (https://12factor.net/config) defines the best
// practice for config in cloud-native apps: store config in the environment.
// This way, the same binary runs in dev (localhost Kafka) and prod (cloud Kafka)
// without code changes—only the environment changes.
//
// WHY not a config file (YAML/TOML)?
// Config files are harder to inject securely in containers and require
// additional parsing libraries. Env vars are universally supported by
// Docker, Kubernetes, and all CI/CD systems.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

// Config is the root configuration struct. Keep it flat enough to pass around
// but structured enough to separate concerns.
type Config struct {
	Kafka  KafkaConfig
	Topics TopicsConfig
	App    AppConfig
}

// KafkaConfig holds all Kafka connection and behavior settings.
type KafkaConfig struct {
	// Brokers is the list of initial bootstrap broker addresses.
	// Only one broker is needed to bootstrap—the client discovers the rest.
	// Format: ["host1:port", "host2:port"]
	Brokers []string

	// Version must match or be lower than your Kafka cluster version.
	// Using a newer version than the broker will cause protocol errors.
	Version sarama.KafkaVersion

	// ConsumerGroupID is the logical name for this group of consumers.
	// All instances with the same GroupID share the topic partitions.
	ConsumerGroupID string

	Producer ProducerCfg
	Consumer ConsumerCfg
}

// ProducerCfg holds producer-specific tuning parameters.
type ProducerCfg struct {
	// RetryMax is how many times to retry a failed produce before returning an error.
	RetryMax int

	// RequiredAcks controls the durability vs. latency trade-off:
	//   NoResponse  (0): fire-and-forget. No guarantee of delivery.
	//   WaitForLocal(1): leader writes to its local log. Good default.
	//   WaitForAll (-1): all in-sync replicas (ISR) must acknowledge. Most durable.
	RequiredAcks sarama.RequiredAcks

	// FlushFrequency is how often the async producer batches and sends messages.
	// Lower = lower latency; Higher = better throughput (more batching).
	FlushFrequency time.Duration

	// Idempotent enables exactly-once delivery at the producer level.
	// The broker assigns a producer ID + sequence numbers to deduplicate retried messages.
	// Requires RequiredAcks = WaitForAll and MaxOpenRequests = 1.
	Idempotent bool

	// TransactionID enables transactional writes (atomic multi-partition/topic).
	// When set, the producer must call BeginTxn/CommitTxn/AbortTxn.
	TransactionID string

	// Compression reduces network bandwidth at cost of CPU.
	// Snappy: good balance of speed and ratio. LZ4: fastest. ZSTD: best ratio.
	Compression sarama.CompressionCodec
}

// ConsumerCfg holds consumer-specific settings.
type ConsumerCfg struct {
	// AutoCommit: when true, offsets are committed automatically on a timer.
	// DANGER: If your app crashes after consuming but before processing,
	// the message is lost. Use false + manual commit for "at-least-once" delivery.
	AutoCommit bool

	// InitialOffset: where to start if no committed offset exists for this group.
	//   OffsetNewest: only new messages (useful for real-time processing)
	//   OffsetOldest: all messages from the beginning (useful for replaying history)
	InitialOffset int64

	// SessionTimeout: Kafka marks a consumer as dead if no heartbeat is received
	// within this window. Triggers a group rebalance.
	SessionTimeout time.Duration

	// HeartbeatInterval: how often the consumer sends heartbeats to Kafka.
	// Must be less than 1/3 of SessionTimeout to avoid spurious rebalances.
	HeartbeatInterval time.Duration
}

// TopicsConfig holds topic name configuration.
// Centralizing topic names here prevents magic strings scattered across the codebase.
type TopicsConfig struct {
	Orders    string
	OrdersDLQ string
	Payments  string
	Events    string
}

// AppConfig holds application-level settings.
type AppConfig struct {
	Environment string
	LogLevel    string
	MetricsPort int
}

// Load reads all configuration from environment variables.
// It first attempts to load a .env file (ignored if absent—useful for Docker/k8s
// where env vars are injected directly).
func Load() (*Config, error) {
	_ = godotenv.Load() // No-op if .env doesn't exist

	kafkaVersion, err := parseKafkaVersion(getEnv("KAFKA_VERSION", "3.6.0"))
	if err != nil {
		return nil, fmt.Errorf("invalid KAFKA_VERSION: %w", err)
	}

	return &Config{
		Kafka: KafkaConfig{
			Brokers:         parseBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
			Version:         kafkaVersion,
			ConsumerGroupID: getEnv("KAFKA_CONSUMER_GROUP", "kafka-learning-group"),
			Producer: ProducerCfg{
				RetryMax:       getEnvInt("KAFKA_PRODUCER_RETRY_MAX", 5),
				RequiredAcks:   parseRequiredAcks(getEnv("KAFKA_PRODUCER_REQUIRED_ACKS", "all")),
				FlushFrequency: getEnvDuration("KAFKA_PRODUCER_FLUSH_FREQUENCY", 500*time.Millisecond),
				Idempotent:     getEnvBool("KAFKA_PRODUCER_IDEMPOTENT", false),
				TransactionID:  getEnv("KAFKA_PRODUCER_TRANSACTION_ID", ""),
				Compression:    parseCompression(getEnv("KAFKA_PRODUCER_COMPRESSION", "snappy")),
			},
			Consumer: ConsumerCfg{
				AutoCommit:        getEnvBool("KAFKA_CONSUMER_AUTO_COMMIT", false),
				InitialOffset:     parseInitialOffset(getEnv("KAFKA_CONSUMER_OFFSET_INITIAL", "newest")),
				SessionTimeout:    getEnvDuration("KAFKA_CONSUMER_SESSION_TIMEOUT", 10*time.Second),
				HeartbeatInterval: getEnvDuration("KAFKA_CONSUMER_HEARTBEAT_INTERVAL", 3*time.Second),
			},
		},
		Topics: TopicsConfig{
			Orders:    getEnv("TOPIC_ORDERS", "orders"),
			OrdersDLQ: getEnv("TOPIC_ORDERS_DLQ", "orders.dlq"),
			Payments:  getEnv("TOPIC_PAYMENTS", "payments"),
			Events:    getEnv("TOPIC_EVENTS", "events"),
		},
		App: AppConfig{
			Environment: getEnv("APP_ENV", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
			MetricsPort: getEnvInt("METRICS_PORT", 2112),
		},
	}, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func parseBrokers(s string) []string {
	parts := strings.Split(s, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}

func parseKafkaVersion(s string) (sarama.KafkaVersion, error) {
	v, err := sarama.ParseKafkaVersion(s)
	if err != nil {
		return sarama.V3_6_0_0, fmt.Errorf("parse %q: %w", s, err)
	}
	return v, nil
}

func parseRequiredAcks(s string) sarama.RequiredAcks {
	switch strings.ToLower(s) {
	case "none", "0":
		return sarama.NoResponse
	case "local", "leader", "1":
		return sarama.WaitForLocal
	default: // "all", "-1"
		return sarama.WaitForAll
	}
}

func parseCompression(s string) sarama.CompressionCodec {
	switch strings.ToLower(s) {
	case "gzip":
		return sarama.CompressionGZIP
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	case "none":
		return sarama.CompressionNone
	default: // "snappy"
		return sarama.CompressionSnappy
	}
}

func parseInitialOffset(s string) int64 {
	if strings.ToLower(s) == "oldest" || strings.ToLower(s) == "earliest" {
		return sarama.OffsetOldest
	}
	return sarama.OffsetNewest
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
