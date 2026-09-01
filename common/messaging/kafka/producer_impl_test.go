package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/testlogger"
)

func TestNewKafkaProducer(t *testing.T) {
	// Create a mock sarama.SyncProducer
	mockProducer := mocks.NewSyncProducer(t, nil)
	logger := testlogger.New(t)
	topic := "test-topic"

	kafkaProducer := NewKafkaProducer(topic, mockProducer, logger)

	// Type assert to *producerImpl to access struct fields
	producedImpl, ok := kafkaProducer.(*producerImpl)
	assert.True(t, ok, "Expected kafkaProducer to be of type *producerImpl")

	// Assert that all fields are correctly set
	assert.Equal(t, topic, producedImpl.topic)
	assert.Equal(t, mockProducer, producedImpl.producer)
	assert.NotNil(t, producedImpl.logger)
	assert.NotNil(t, producedImpl.msgEncoder)
}

func TestPublish(t *testing.T) {
	msgType := indexer.MessageTypeIndex
	testCases := []struct {
		name    string
		message interface{}
		hasErr  bool
	}{
		{
			name: "Publish message succeeded",
			message: &indexer.Message{
				MessageType: &msgType,
				DomainID:    common.StringPtr("test-domain-id"),
				WorkflowID:  common.StringPtr("test-workflow-id"),
				RunID:       common.StringPtr("test-workflow-run-id"),
			},
			hasErr: false,
		},
		{
			name:    "Unrecognized message type",
			message: "This is not a recognized message type",
			hasErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock sarama.SyncProducer
			mockProducer := mocks.NewSyncProducer(t, nil)
			logger := testlogger.New(t)
			topic := "test-topic"
			ctx := context.Background()
			kafkaProducer := NewKafkaProducer(topic, mockProducer, logger)

			// Only expect SendMessage to be called if no error is expected
			if !tc.hasErr {
				mockProducer.ExpectSendMessageAndSucceed()
			}
			err := kafkaProducer.Publish(ctx, tc.message)

			if !tc.hasErr {
				assert.NoError(t, err, "An error was not expected but got %v", err)
			} else {
				assert.Error(t, err, "Expected an error but none was found")
			}
		})
	}
}
