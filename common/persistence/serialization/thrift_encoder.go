package serialization

import (
	"bytes"

	"go.uber.org/thriftrw/protocol/binary"

	"github.com/uber/cadence/common/constants"
)

type thriftEncoder struct{}

func newThriftEncoder() encoder {
	return &thriftEncoder{}
}

func (e *thriftEncoder) shardInfoToBlob(info *ShardInfo) ([]byte, error) {
	return thriftRWEncode(shardInfoToThrift(info))
}

func (e *thriftEncoder) domainInfoToBlob(info *DomainInfo) ([]byte, error) {
	return thriftRWEncode(domainInfoToThrift(info))
}

func (e *thriftEncoder) historyTreeInfoToBlob(info *HistoryTreeInfo) ([]byte, error) {
	return thriftRWEncode(historyTreeInfoToThrift(info))
}

func (e *thriftEncoder) workflowExecutionInfoToBlob(info *WorkflowExecutionInfo) ([]byte, error) {
	return thriftRWEncode(workflowExecutionInfoToThrift(info))
}

func (e *thriftEncoder) activityInfoToBlob(info *ActivityInfo) ([]byte, error) {
	return thriftRWEncode(activityInfoToThrift(info))
}

func (e *thriftEncoder) childExecutionInfoToBlob(info *ChildExecutionInfo) ([]byte, error) {
	return thriftRWEncode(childExecutionInfoToThrift(info))
}

func (e *thriftEncoder) signalInfoToBlob(info *SignalInfo) ([]byte, error) {
	return thriftRWEncode(signalInfoToThrift(info))
}

func (e *thriftEncoder) requestCancelInfoToBlob(info *RequestCancelInfo) ([]byte, error) {
	return thriftRWEncode(requestCancelInfoToThrift(info))
}

func (e *thriftEncoder) timerInfoToBlob(info *TimerInfo) ([]byte, error) {
	return thriftRWEncode(timerInfoToThrift(info))
}

func (e *thriftEncoder) taskInfoToBlob(info *TaskInfo) ([]byte, error) {
	return thriftRWEncode(taskInfoToThrift(info))
}

func (e *thriftEncoder) taskListInfoToBlob(info *TaskListInfo) ([]byte, error) {
	return thriftRWEncode(taskListInfoToThrift(info))
}

func (e *thriftEncoder) transferTaskInfoToBlob(info *TransferTaskInfo) ([]byte, error) {
	return thriftRWEncode(transferTaskInfoToThrift(info))
}

func (e *thriftEncoder) crossClusterTaskInfoToBlob(info *CrossClusterTaskInfo) ([]byte, error) {
	return thriftRWEncode(crossClusterTaskInfoToThrift(info))
}

func (e *thriftEncoder) timerTaskInfoToBlob(info *TimerTaskInfo) ([]byte, error) {
	return thriftRWEncode(timerTaskInfoToThrift(info))
}

func (e *thriftEncoder) replicationTaskInfoToBlob(info *ReplicationTaskInfo) ([]byte, error) {
	return thriftRWEncode(replicationTaskInfoToThrift(info))
}

func (e *thriftEncoder) encodingType() constants.EncodingType {
	return constants.EncodingTypeThriftRW
}

func thriftRWEncode(t thriftRWType) ([]byte, error) {
	var b bytes.Buffer
	sw := binary.Default.Writer(&b)
	defer sw.Close()
	if err := t.Encode(sw); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
