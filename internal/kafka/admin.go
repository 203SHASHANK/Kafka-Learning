package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"kafka-learning/internal/config"
)

// Admin wraps sarama's ClusterAdmin for topic and group management.
//
// In production, topic creation is typically handled by:
//   - Terraform / Pulumi (infrastructure as code)
//   - Kafka AdminClient in CI/CD pipelines
//   - Confluent Schema Registry automation
//
// For learning and local development, programmatic creation is convenient.
// This wrapper handles "already exists" gracefully and adds logging.
type Admin struct {
	admin  sarama.ClusterAdmin
	logger *zap.Logger
}

// NewAdmin creates an admin client.
func NewAdmin(cfg *config.Config) (*Admin, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = cfg.Kafka.Version

	admin, err := sarama.NewClusterAdmin(cfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating cluster admin: %w", err)
	}

	return &Admin{
		admin:  admin,
		logger: zap.L().With(zap.String("component", "kafka-admin")),
	}, nil
}

// CreateTopic creates a topic. Returns nil if the topic already exists (idempotent).
func (a *Admin) CreateTopic(cfg TopicConfig) error {
	detail := &sarama.TopicDetail{
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ConfigEntries:     make(map[string]*string),
	}

	if cfg.RetentionMs > 0 {
		s := fmt.Sprintf("%d", cfg.RetentionMs)
		detail.ConfigEntries["retention.ms"] = &s
	}

	if cfg.CleanupPolicy != "" {
		detail.ConfigEntries["cleanup.policy"] = &cfg.CleanupPolicy
	}

	err := a.admin.CreateTopic(cfg.Name, detail, false)
	if err != nil {
		if isTopicExists(err) {
			a.logger.Info("topic already exists, skipping", zap.String("topic", cfg.Name))
			return nil
		}
		return fmt.Errorf("creating topic %q: %w", cfg.Name, err)
	}

	a.logger.Info("topic created",
		zap.String("topic", cfg.Name),
		zap.Int32("partitions", cfg.Partitions),
		zap.Int16("replication_factor", cfg.ReplicationFactor),
	)
	return nil
}

// CreateTopics creates multiple topics. Continues on "already exists" errors.
func (a *Admin) CreateTopics(cfgs []TopicConfig) error {
	for _, cfg := range cfgs {
		if err := a.CreateTopic(cfg); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopic deletes a topic. Use with caution—this is irreversible.
func (a *Admin) DeleteTopic(name string) error {
	if err := a.admin.DeleteTopic(name); err != nil {
		return fmt.Errorf("deleting topic %q: %w", name, err)
	}
	a.logger.Info("topic deleted", zap.String("topic", name))
	return nil
}

// ListTopics returns all topic names in the cluster.
func (a *Admin) ListTopics() ([]string, error) {
	topics, err := a.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	return names, nil
}

// DescribeConsumerGroup returns metadata for a consumer group.
// Useful for monitoring consumer lag and understanding group state.
func (a *Admin) DescribeConsumerGroup(groupID string) (*ConsumerGroupInfo, error) {
	groups, err := a.admin.DescribeConsumerGroups([]string{groupID})
	if err != nil {
		return nil, fmt.Errorf("describing group %q: %w", groupID, err)
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("group %q not found", groupID)
	}

	g := groups[0]
	info := &ConsumerGroupInfo{
		GroupID: g.GroupId,
		State:   g.State,
	}
	for _, m := range g.Members {
		info.Members = append(info.Members, ConsumerGroupMember{
			ClientID:   m.ClientId,
			ClientHost: m.ClientHost,
		})
	}
	return info, nil
}

// Close releases the admin client connection.
func (a *Admin) Close() error {
	return a.admin.Close()
}

// isTopicExists checks if a Kafka error means the topic already exists.
func isTopicExists(err error) bool {
	if kafkaErr, ok := err.(*sarama.TopicError); ok {
		return kafkaErr.Err == sarama.ErrTopicAlreadyExists
	}
	return false
}
