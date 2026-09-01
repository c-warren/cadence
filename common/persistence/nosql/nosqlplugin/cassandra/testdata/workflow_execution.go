package testdata

import (
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

type WFExecRequestOption func(*nosqlplugin.WorkflowExecutionRequest)

func WFExecRequestWithMapsWriteMode(mode nosqlplugin.WorkflowExecutionMapsWriteMode) WFExecRequestOption {
	return func(request *nosqlplugin.WorkflowExecutionRequest) {
		request.MapsWriteMode = mode
	}
}

func WFExecRequestWithEventBufferWriteMode(mode nosqlplugin.EventBufferWriteMode) WFExecRequestOption {
	return func(request *nosqlplugin.WorkflowExecutionRequest) {
		request.EventBufferWriteMode = mode
	}
}

func WFExecRequest(opts ...WFExecRequestOption) *nosqlplugin.WorkflowExecutionRequest {
	ts := time.Now()
	req := &nosqlplugin.WorkflowExecutionRequest{
		InternalWorkflowExecutionInfo: persistence.InternalWorkflowExecutionInfo{
			DomainID:   "test-domain-id",
			WorkflowID: "test-workflow-id",
			CompletionEvent: &persistence.DataBlob{
				Encoding: constants.EncodingTypeThriftRW,
				Data:     []byte("test-completion-event"),
			},
			AutoResetPoints: &persistence.DataBlob{
				Encoding: constants.EncodingTypeThriftRW,
				Data:     []byte("test-auto-reset-points"),
			},
		},
		VersionHistories: &persistence.DataBlob{
			Encoding: constants.EncodingTypeThriftRW,
			Data:     []byte("test-version-histories"),
		},
		Checksums: &checksum.Checksum{
			Version: 1,
			Flavor:  checksum.FlavorIEEECRC32OverThriftBinary,
			Value:   []byte("test-checksum"),
		},
		PreviousNextEventIDCondition: common.Int64Ptr(123),
		CurrentTimeStamp:             ts,
	}

	for _, opt := range opts {
		opt(req)
	}

	return req
}
