package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
)

func TestDefaultKafkaVersion(t *testing.T) {
	// Verify the default Kafka version constant is a valid, parseable version.
	_, err := sarama.ParseKafkaVersion(defaultKafkaVersion)
	assert.NoError(t, err, "defaultKafkaVersion %q must be parseable by sarama", defaultKafkaVersion)
}

func TestNewKafkaClient(t *testing.T) {
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History, metrics.MigrationConfig{})
	logger := testlogger.New(t)
	testCases := []struct {
		name        string
		config      *config.KafkaConfig
		checkApp    bool
		expectedErr string
	}{
		{
			name: "Missing clusters",
			config: &config.KafkaConfig{
				Clusters: map[string]config.ClusterConfig{},
			},
			checkApp:    true,
			expectedErr: "Empty Kafka Cluster Config",
		},
		{
			name: "Missing topics",
			config: &config.KafkaConfig{
				Clusters: map[string]config.ClusterConfig{
					"testCluster": {
						Brokers: []string{"testBrokers"},
					},
				},
				Topics: map[string]config.TopicConfig{},
			},
			checkApp:    true,
			expectedErr: "Empty Topics Config",
		},
		{
			name: "Missing Applications",
			config: &config.KafkaConfig{
				Clusters: map[string]config.ClusterConfig{
					"test-cluster": {
						Brokers: []string{"test-brokers"},
					},
				},
				Topics: map[string]config.TopicConfig{
					"test-topic": {
						Cluster: "test-cluster",
					},
				},
				Applications: map[string]config.TopicList{},
			},
			checkApp:    true,
			expectedErr: "Empty Applications Config",
		},
		{
			name: "Missing topics config",
			config: &config.KafkaConfig{
				Clusters: map[string]config.ClusterConfig{
					"test-cluster": {
						Brokers: []string{"test-brokers"},
					},
				},
				Topics: map[string]config.TopicConfig{
					"test-topic": {
						Cluster: "test-cluster",
					},
				},
				Applications: map[string]config.TopicList{
					"test-app": {
						Topic:    "test-topic",
						DLQTopic: "test-topic-dlq",
					},
				},
			},
			checkApp:    true,
			expectedErr: "Missing Topic Config for Topic test-topic-dlq",
		},
		{
			name: "Normal Case",
			config: &config.KafkaConfig{
				Clusters: map[string]config.ClusterConfig{
					"test-cluster": {
						Brokers: []string{"test-brokers"},
					},
				},
				Topics: map[string]config.TopicConfig{
					"test-topic": {
						Cluster: "test-cluster",
					},
					"test-topic-dlq": {
						Cluster: "test-cluster",
					},
				},
				Applications: map[string]config.TopicList{
					"test-app": {
						Topic:    "test-topic",
						DLQTopic: "test-topic-dlq",
					},
				},
			},
			checkApp: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, tc.expectedErr, r)
				}
			}()
			kafkaClient := NewKafkaClient(tc.config, metricsClient, logger, nil, tc.checkApp)
			// Type assert to *clientImpl to access struct fields
			client, ok := kafkaClient.(*clientImpl)
			assert.True(t, ok, "Expected kafkaClient to be of type *clientImpl")
			assert.Equal(t, tc.config, client.config)
		})
	}
}
