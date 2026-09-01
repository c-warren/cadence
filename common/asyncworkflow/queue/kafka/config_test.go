package kafka

import "testing"

func TestQueueConfigID(t *testing.T) {
	tests := []struct {
		name     string
		config   queueConfig
		expected string
	}{
		{
			name: "single broker",
			config: queueConfig{
				Connection: connectionConfig{
					Brokers: []string{"broker1:9092"},
				},
				Topic: "topic1",
			},
			expected: "kafka::topic1/broker1:9092",
		},
		{
			name: "multiple brokers",
			config: queueConfig{
				Connection: connectionConfig{
					Brokers: []string{"broker1:9092", "broker2:9092"},
				},
				Topic: "topic2",
			},
			expected: "kafka::topic2/broker1:9092,broker2:9092",
		},
		{
			name: "no brokers",
			config: queueConfig{
				Connection: connectionConfig{
					Brokers: []string{},
				},
				Topic: "topic3",
			},
			expected: "kafka::topic3/",
		},
		{
			name: "empty topic",
			config: queueConfig{
				Connection: connectionConfig{
					Brokers: []string{"broker1:9092"},
				},
				Topic: "",
			},
			expected: "kafka::/broker1:9092",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.ID()
			if got != tt.expected {
				t.Errorf("queueConfig.ID() = %v, want %v", got, tt.expected)
			}
		})
	}
}
