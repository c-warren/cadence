package analytics

import (
	"context"
	"encoding/json"

	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/messaging"
)

type Emitter DataEmitter

type emitter struct {
	producer messaging.Producer
}

type EmitterParams struct {
	Producer messaging.Producer
}

func NewEmitter(p EmitterParams) DataEmitter {
	return &emitter{
		producer: p.Producer,
	}
}

func (et *emitter) EmitUsageData(ctx context.Context, data WfDiagnosticsUsageData) error {
	msg := make(map[string]interface{})
	msg[Domain] = data.Domain
	msg[WorkflowID] = data.WorkflowID
	msg[RunID] = data.RunID
	msg[Identity] = data.Identity
	msg[SatisfactionFeedback] = data.SatisfactionFeedback
	msg[IssueType] = data.IssueType
	msg[DiagnosticsWfID] = data.DiagnosticsWorkflowID
	msg[DiagnosticsWfRunID] = data.DiagnosticsRunID
	msg[Environment] = data.Environment
	msg[DiagnosticsStartTime] = data.DiagnosticsStartTime.UTC().UnixMilli()
	msg[DiagnosticsEndTime] = data.DiagnosticsEndTime.UTC().UnixMilli()

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	pinotMsg := &indexer.PinotMessage{
		WorkflowID: common.StringPtr(data.DiagnosticsWorkflowID),
		Payload:    serializedMsg,
	}
	return et.producer.Publish(ctx, pinotMsg)
}
