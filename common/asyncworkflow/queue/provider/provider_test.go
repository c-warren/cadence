package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/types"
)

func TestQueueProvider(t *testing.T) {
	testCases := []struct {
		name      string
		queueType string
		setup     func()
		wantErr   bool
	}{
		{
			name:      "Success case",
			queueType: "q1",
			wantErr:   false,
		},
		{
			name:      "Duplicate type",
			queueType: "q2",
			setup: func() {
				RegisterQueueProvider("q2", func(Decoder) (Queue, error) {
					return nil, nil
				})
			},
			wantErr: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := GetQueueProvider(tt.queueType)
			assert.False(t, ok)

			if tt.setup != nil {
				tt.setup()
			}

			err := RegisterQueueProvider(tt.queueType, func(Decoder) (Queue, error) {
				return nil, nil
			})
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			_, ok = GetQueueProvider(tt.queueType)
			assert.True(t, ok)
		})
	}
}

func TestDecoder(t *testing.T) {
	testCases := []struct {
		name      string
		queueType string
		setup     func()
		wantErr   bool
	}{
		{
			name:      "Success case",
			queueType: "q1",
			wantErr:   false,
		},
		{
			name:      "Duplicate type",
			queueType: "q2",
			setup: func() {
				RegisterDecoder("q2", func(*types.DataBlob) Decoder {
					return nil
				})
			},
			wantErr: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := GetDecoder(tt.queueType)
			assert.False(t, ok)

			if tt.setup != nil {
				tt.setup()
			}

			err := RegisterDecoder(tt.queueType, func(*types.DataBlob) Decoder {
				return nil
			})
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			_, ok = GetDecoder(tt.queueType)
			assert.True(t, ok)
		})
	}
}
