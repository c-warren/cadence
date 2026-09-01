package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/asyncworkflow/queue/provider"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/config/yaml"
	"github.com/uber/cadence/common/types"
)

func mockQueueConstructor(cfg provider.Decoder) (provider.Queue, error) {
	// Mock implementation
	return nil, nil
}

func mockDecoderConstructor(cfg *types.DataBlob) provider.Decoder {
	// Mock implementation
	return nil
}

func TestNewAsyncQueueProvider(t *testing.T) {
	// Mock the provider registration functions
	provider.RegisterQueueProvider("validType", mockQueueConstructor)
	provider.RegisterDecoder("validType", mockDecoderConstructor)

	tests := []struct {
		name          string
		cfg           map[string]config.AsyncWorkflowQueueProvider
		expectError   bool
		errorContains string
	}{
		{
			name: "Successful Initialization",
			cfg: map[string]config.AsyncWorkflowQueueProvider{
				"testQueue": {Type: "validType", Config: &yaml.Node{}},
			},
			expectError: false,
		},
		{
			name: "Unregistered Queue Type",
			cfg: map[string]config.AsyncWorkflowQueueProvider{
				"testQueue": {Type: "invalidType", Config: &yaml.Node{}},
			},
			expectError:   true,
			errorContains: "not registered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAsyncQueueProvider(tt.cfg)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetPredefinedQueue(t *testing.T) {
	p := &providerImpl{
		queues: map[string]provider.Queue{
			"testQueue": nil,
		},
	}

	tests := []struct {
		name          string
		queueName     string
		expectError   bool
		errorContains string
	}{
		{
			name:        "Successful Get",
			queueName:   "testQueue",
			expectError: false,
		},
		{
			name:          "Queue Not Found",
			queueName:     "invalidQueue",
			expectError:   true,
			errorContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.GetPredefinedQueue(tt.queueName)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetQueue(t *testing.T) {
	// Mock the provider registration functions
	provider.RegisterQueueProvider("validType", mockQueueConstructor)
	provider.RegisterDecoder("validType", mockDecoderConstructor)

	p := &providerImpl{
		queues: map[string]provider.Queue{},
	}

	tests := []struct {
		name          string
		queueType     string
		queueConfig   *types.DataBlob
		expectError   bool
		errorContains string
	}{
		{
			name:        "Successful Get",
			queueType:   "validType",
			queueConfig: &types.DataBlob{},
			expectError: false,
		},
		{
			name:          "Unregistered Queue Type",
			queueType:     "invalidType",
			queueConfig:   &types.DataBlob{},
			expectError:   true,
			errorContains: "not registered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.GetQueue(tt.queueType, tt.queueConfig)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
