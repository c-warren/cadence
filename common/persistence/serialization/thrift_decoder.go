package serialization

import (
	"bytes"

	"go.uber.org/thriftrw/protocol/binary"

	"github.com/uber/cadence/.gen/go/sqlblobs"
)

type (
	thriftDecoder struct{}
)

func newThriftDecoder() decoder {
	return &thriftDecoder{}
}

func (d *thriftDecoder) shardInfoFromBlob(data []byte) (*ShardInfo, error) {
	result := &sqlblobs.ShardInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return shardInfoFromThrift(result), nil
}

func (d *thriftDecoder) domainInfoFromBlob(data []byte) (*DomainInfo, error) {
	result := &sqlblobs.DomainInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return domainInfoFromThrift(result), nil
}

func (d *thriftDecoder) historyTreeInfoFromBlob(data []byte) (*HistoryTreeInfo, error) {
	result := &sqlblobs.HistoryTreeInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return historyTreeInfoFromThrift(result), nil
}

func (d *thriftDecoder) workflowExecutionInfoFromBlob(data []byte) (*WorkflowExecutionInfo, error) {
	result := &sqlblobs.WorkflowExecutionInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return workflowExecutionInfoFromThrift(result), nil
}

func (d *thriftDecoder) activityInfoFromBlob(data []byte) (*ActivityInfo, error) {
	result := &sqlblobs.ActivityInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return activityInfoFromThrift(result), nil
}

func (d *thriftDecoder) childExecutionInfoFromBlob(data []byte) (*ChildExecutionInfo, error) {
	result := &sqlblobs.ChildExecutionInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return childExecutionInfoFromThrift(result), nil
}

func (d *thriftDecoder) signalInfoFromBlob(data []byte) (*SignalInfo, error) {
	result := &sqlblobs.SignalInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return signalInfoFromThrift(result), nil
}

func (d *thriftDecoder) requestCancelInfoFromBlob(data []byte) (*RequestCancelInfo, error) {
	result := &sqlblobs.RequestCancelInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return requestCancelInfoFromThrift(result), nil
}

func (d *thriftDecoder) timerInfoFromBlob(data []byte) (*TimerInfo, error) {
	result := &sqlblobs.TimerInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return timerInfoFromThrift(result), nil
}

func (d *thriftDecoder) taskInfoFromBlob(data []byte) (*TaskInfo, error) {
	result := &sqlblobs.TaskInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return taskInfoFromThrift(result), nil
}

func (d *thriftDecoder) taskListInfoFromBlob(data []byte) (*TaskListInfo, error) {
	result := &sqlblobs.TaskListInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return taskListInfoFromThrift(result), nil
}

func (d *thriftDecoder) transferTaskInfoFromBlob(data []byte) (*TransferTaskInfo, error) {
	result := &sqlblobs.TransferTaskInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return transferTaskInfoFromThrift(result), nil
}

func (d *thriftDecoder) crossClusterTaskInfoFromBlob(data []byte) (*CrossClusterTaskInfo, error) {
	result := &sqlblobsCrossClusterTaskInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return crossClusterTaskInfoFromThrift(result), nil
}

func (d *thriftDecoder) timerTaskInfoFromBlob(data []byte) (*TimerTaskInfo, error) {
	result := &sqlblobs.TimerTaskInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return timerTaskInfoFromThrift(result), nil
}

func (d *thriftDecoder) replicationTaskInfoFromBlob(data []byte) (*ReplicationTaskInfo, error) {
	result := &sqlblobs.ReplicationTaskInfo{}
	if err := thriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return replicationTaskInfoFromThrift(result), nil
}

func thriftRWDecode(b []byte, result thriftRWType) error {
	buf := bytes.NewReader(b)
	sr := binary.Default.Reader(buf)
	return result.Decode(sr)
}
