package analytics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/common/mocks"
)

func Test__EmitUsageData(t *testing.T) {

	testdata := WfDiagnosticsUsageData{
		Domain:                "test-domain",
		WorkflowID:            "wid",
		RunID:                 "rid",
		Identity:              "test@uber.com",
		IssueType:             "timeout",
		DiagnosticsWorkflowID: "diagnostics-wid",
		DiagnosticsRunID:      "diagnostics-rid",
		Environment:           "test-env",
		DiagnosticsStartTime:  time.Now(),
		DiagnosticsEndTime:    time.Now().Add(1 * time.Minute),
		SatisfactionFeedback:  false,
	}
	mockErr := errors.New("mockErr")
	tests := map[string]struct {
		data                   WfDiagnosticsUsageData
		producerMockAffordance func(mockProducer *mocks.KafkaProducer)
		expectedError          error
	}{
		"Case1: normal case": {
			data: testdata,
			producerMockAffordance: func(mockProducer *mocks.KafkaProducer) {
				mockProducer.On("Publish", mock.Anything, mock.MatchedBy(func(input *indexer.PinotMessage) bool {
					require.Equal(t, testdata.DiagnosticsWorkflowID, input.GetWorkflowID())
					return true
				})).Return(nil).Once()
			},
			expectedError: nil,
		},
		"Case1: error case": {
			data: testdata,
			producerMockAffordance: func(mockProducer *mocks.KafkaProducer) {
				mockProducer.On("Publish", mock.Anything, mock.MatchedBy(func(input *indexer.PinotMessage) bool {
					require.Equal(t, testdata.DiagnosticsWorkflowID, input.GetWorkflowID())
					return true
				})).Return(mockErr).Once()
			},
			expectedError: mockErr,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mockProducer := &mocks.KafkaProducer{}
			emitter := NewEmitter(EmitterParams{
				Producer: mockProducer,
			})
			test.producerMockAffordance(mockProducer)

			err := emitter.EmitUsageData(context.Background(), test.data)
			if test.expectedError != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
