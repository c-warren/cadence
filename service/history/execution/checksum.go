package execution

import (
	"fmt"
	"slices"

	checksumgen "github.com/uber/cadence/.gen/go/checksum"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/types/mapper/thrift"
)

const (
	mutableStateChecksumPayloadV1 = 1
)

func generateMutableStateChecksum(ms MutableState) (checksum.Checksum, error) {
	payload := newMutableStateChecksumPayload(ms)
	csum, err := checksum.GenerateCRC32(payload, mutableStateChecksumPayloadV1)
	if err != nil {
		return checksum.Checksum{}, err
	}
	return csum, nil
}

func verifyMutableStateChecksum(
	ms MutableState,
	csum checksum.Checksum,
) error {
	if csum.Version != mutableStateChecksumPayloadV1 {
		return fmt.Errorf("invalid checksum payload version %v", csum.Version)
	}
	payload := newMutableStateChecksumPayload(ms)
	return checksum.Verify(payload, csum)
}

func newMutableStateChecksumPayload(ms MutableState) *checksumgen.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	payload := &checksumgen.MutableStateChecksumPayload{
		CancelRequested:      common.BoolPtr(executionInfo.CancelRequested),
		State:                common.Int16Ptr(int16(executionInfo.State)),
		LastFirstEventID:     common.Int64Ptr(executionInfo.LastFirstEventID),
		NextEventID:          common.Int64Ptr(executionInfo.NextEventID),
		LastProcessedEventID: common.Int64Ptr(executionInfo.LastProcessedEvent),
		SignalCount:          common.Int64Ptr(int64(executionInfo.SignalCount)),
		DecisionAttempt:      common.Int32Ptr(int32(executionInfo.DecisionAttempt)),
		DecisionScheduledID:  common.Int64Ptr(executionInfo.DecisionScheduleID),
		DecisionStartedID:    common.Int64Ptr(executionInfo.DecisionStartedID),
		DecisionVersion:      common.Int64Ptr(executionInfo.DecisionVersion),
		StickyTaskListName:   common.StringPtr(executionInfo.StickyTaskList),
	}

	versionHistories := ms.GetVersionHistories()
	if versionHistories != nil {
		payload.VersionHistories = thrift.FromVersionHistories(versionHistories.ToInternalType())
	}

	// for each of the pendingXXX ids below, sorting is needed to guarantee that
	// same serialized bytes can be generated during verification
	pendingTimerIDs := make([]int64, 0, len(ms.GetPendingTimerInfos()))
	for _, ti := range ms.GetPendingTimerInfos() {
		pendingTimerIDs = append(pendingTimerIDs, ti.StartedID)
	}
	slices.Sort(pendingTimerIDs)
	payload.PendingTimerStartedIDs = pendingTimerIDs

	pendingActivityIDs := make([]int64, 0, len(ms.GetPendingActivityInfos()))
	for id := range ms.GetPendingActivityInfos() {
		pendingActivityIDs = append(pendingActivityIDs, id)
	}
	slices.Sort(pendingActivityIDs)
	payload.PendingActivityScheduledIDs = pendingActivityIDs

	pendingChildIDs := make([]int64, 0, len(ms.GetPendingChildExecutionInfos()))
	for id := range ms.GetPendingChildExecutionInfos() {
		pendingChildIDs = append(pendingChildIDs, id)
	}
	slices.Sort(pendingChildIDs)
	payload.PendingChildInitiatedIDs = pendingChildIDs

	signalIDs := make([]int64, 0, len(ms.GetPendingSignalExternalInfos()))
	for id := range ms.GetPendingSignalExternalInfos() {
		signalIDs = append(signalIDs, id)
	}
	slices.Sort(signalIDs)
	payload.PendingSignalInitiatedIDs = signalIDs

	requestCancelIDs := make([]int64, 0, len(ms.GetPendingRequestCancelExternalInfos()))
	for id := range ms.GetPendingRequestCancelExternalInfos() {
		requestCancelIDs = append(requestCancelIDs, id)
	}
	slices.Sort(requestCancelIDs)
	payload.PendingReqCancelInitiatedIDs = requestCancelIDs
	return payload
}
