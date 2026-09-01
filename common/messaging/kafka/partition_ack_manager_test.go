package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
)

func TestAddMessage(t *testing.T) {
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History, metrics.MigrationConfig{})
	logger := testlogger.New(t) // Mocked
	pam := newPartitionAckManager(metricsClient, logger)
	partitionID := int32(1)
	messageID := int64(100)
	// Test adding a message
	pam.AddMessage(partitionID, messageID)

	// Verify the message is added to the ack manager
	pam.RLock() // Read lock since we are only reading data
	_, ok := pam.ackMgrs[partitionID]
	pam.RUnlock()

	assert.True(t, ok, "AckManager for partition %v was not created", partitionID)
}

func TestCompleteMessage(t *testing.T) {
	// Setup
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History, metrics.MigrationConfig{})
	logger := testlogger.New(t)
	pam := newPartitionAckManager(metricsClient, logger)

	partitionID := int32(1)
	messageID := int64(100)

	testCases := []struct {
		name        string
		partitionID int32
		messageID   int64
		isAck       bool
		expected    int64
		hasErr      bool
	}{
		{
			name:        "Acknowledge the message",
			partitionID: partitionID,
			messageID:   messageID,
			isAck:       true,
			expected:    int64(100),
			hasErr:      false,
		},
		{
			name:        "Not acknowledge the message",
			partitionID: partitionID,
			messageID:   messageID,
			isAck:       false,
			expected:    int64(100),
			hasErr:      false,
		},
		{
			name:        "Not exist partition",
			partitionID: partitionID + 1,
			messageID:   messageID,
			isAck:       true,
			expected:    int64(-1),
			hasErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Add a message first to simulate a real-world scenario, this will create ackMgr for partition
			pam.AddMessage(partitionID, messageID)

			ackLevel, err := pam.CompleteMessage(tc.partitionID, tc.messageID, tc.isAck)
			assert.True(t, ackLevel == tc.expected, "Test case %s failed: expected ackLevel %d, got %d", tc.name, tc.expected, ackLevel)

			if tc.hasErr {
				assert.Error(t, err, "Expected an error but none was found")
			} else {
				assert.NoError(t, err, "An error was not expected but got %v", err)
			}
		})
	}
}
