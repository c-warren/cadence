package serialization

import (
	"bytes"

	"github.com/golang/snappy"
	"go.uber.org/thriftrw/protocol/binary"

	"github.com/uber/cadence/common/constants"
)

type snappyThriftEncoder struct{}

func newSnappyThriftEncoder() encoder {
	return &snappyThriftEncoder{}
}

func (e *snappyThriftEncoder) shardInfoToBlob(info *ShardInfo) ([]byte, error) {
	return snappyThriftRWEncode(shardInfoToThrift(info))
}

func (e *snappyThriftEncoder) domainInfoToBlob(info *DomainInfo) ([]byte, error) {
	return snappyThriftRWEncode(domainInfoToThrift(info))
}

func (e *snappyThriftEncoder) historyTreeInfoToBlob(info *HistoryTreeInfo) ([]byte, error) {
	return snappyThriftRWEncode(historyTreeInfoToThrift(info))
}

func (e *snappyThriftEncoder) workflowExecutionInfoToBlob(info *WorkflowExecutionInfo) ([]byte, error) {
	return snappyThriftRWEncode(workflowExecutionInfoToThrift(info))
}

func (e *snappyThriftEncoder) activityInfoToBlob(info *ActivityInfo) ([]byte, error) {
	return snappyThriftRWEncode(activityInfoToThrift(info))
}

func (e *snappyThriftEncoder) childExecutionInfoToBlob(info *ChildExecutionInfo) ([]byte, error) {
	return snappyThriftRWEncode(childExecutionInfoToThrift(info))
}

func (e *snappyThriftEncoder) signalInfoToBlob(info *SignalInfo) ([]byte, error) {
	return snappyThriftRWEncode(signalInfoToThrift(info))
}

func (e *snappyThriftEncoder) requestCancelInfoToBlob(info *RequestCancelInfo) ([]byte, error) {
	return snappyThriftRWEncode(requestCancelInfoToThrift(info))
}

func (e *snappyThriftEncoder) timerInfoToBlob(info *TimerInfo) ([]byte, error) {
	return snappyThriftRWEncode(timerInfoToThrift(info))
}

func (e *snappyThriftEncoder) taskInfoToBlob(info *TaskInfo) ([]byte, error) {
	return snappyThriftRWEncode(taskInfoToThrift(info))
}

func (e *snappyThriftEncoder) taskListInfoToBlob(info *TaskListInfo) ([]byte, error) {
	return snappyThriftRWEncode(taskListInfoToThrift(info))
}

func (e *snappyThriftEncoder) transferTaskInfoToBlob(info *TransferTaskInfo) ([]byte, error) {
	return snappyThriftRWEncode(transferTaskInfoToThrift(info))
}

func (e *snappyThriftEncoder) crossClusterTaskInfoToBlob(info *CrossClusterTaskInfo) ([]byte, error) {
	return snappyThriftRWEncode(crossClusterTaskInfoToThrift(info))
}

func (e *snappyThriftEncoder) timerTaskInfoToBlob(info *TimerTaskInfo) ([]byte, error) {
	return snappyThriftRWEncode(timerTaskInfoToThrift(info))
}

func (e *snappyThriftEncoder) replicationTaskInfoToBlob(info *ReplicationTaskInfo) ([]byte, error) {
	return snappyThriftRWEncode(replicationTaskInfoToThrift(info))
}

func (e *snappyThriftEncoder) encodingType() constants.EncodingType {
	return constants.EncodingTypeThriftRWSnappy
}

func snappyThriftRWEncode(t thriftRWType) ([]byte, error) {
	var b bytes.Buffer
	sw := binary.Default.Writer(&b)
	defer sw.Close()
	if err := t.Encode(sw); err != nil {
		return nil, err
	}

	return snappy.Encode(nil, b.Bytes()), nil
}
