package queue

import (
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

func convertToPersistenceTransferProcessingQueueStates(states []ProcessingQueueState) []*types.ProcessingQueueState {
	pStates := make([]*types.ProcessingQueueState, 0, len(states))
	for _, state := range states {
		pStates = append(pStates, &types.ProcessingQueueState{
			Level:        common.Int32Ptr(int32(state.Level())),
			AckLevel:     common.Int64Ptr(state.AckLevel().(transferTaskKey).taskID),
			MaxLevel:     common.Int64Ptr(state.MaxLevel().(transferTaskKey).taskID),
			DomainFilter: convertToPersistenceDomainFilter(state.DomainFilter()),
		})
	}

	return pStates
}

func convertFromPersistenceTransferProcessingQueueStates(pStates []*types.ProcessingQueueState) []ProcessingQueueState {
	states := make([]ProcessingQueueState, 0, len(pStates))
	for _, pState := range pStates {
		states = append(states, NewProcessingQueueState(
			int(pState.GetLevel()),
			newTransferTaskKey(pState.GetAckLevel()),
			newTransferTaskKey(pState.GetMaxLevel()),
			convertFromPersistenceDomainFilter(pState.DomainFilter),
		))
	}

	return states
}

func convertToPersistenceTimerProcessingQueueStates(states []ProcessingQueueState) []*types.ProcessingQueueState {
	pStates := make([]*types.ProcessingQueueState, 0, len(states))
	for _, state := range states {
		pStates = append(pStates, &types.ProcessingQueueState{
			Level:        common.Int32Ptr(int32(state.Level())),
			AckLevel:     common.Int64Ptr(state.AckLevel().(timerTaskKey).visibilityTimestamp.UnixNano()),
			MaxLevel:     common.Int64Ptr(state.MaxLevel().(timerTaskKey).visibilityTimestamp.UnixNano()),
			DomainFilter: convertToPersistenceDomainFilter(state.DomainFilter()),
		})
	}

	return pStates
}

func convertFromPersistenceTimerProcessingQueueStates(pStates []*types.ProcessingQueueState) []ProcessingQueueState {
	states := make([]ProcessingQueueState, 0, len(pStates))
	for _, pState := range pStates {
		states = append(states, NewProcessingQueueState(
			int(pState.GetLevel()),
			newTimerTaskKey(time.Unix(0, pState.GetAckLevel()), 0),
			newTimerTaskKey(time.Unix(0, pState.GetMaxLevel()), 0),
			convertFromPersistenceDomainFilter(pState.DomainFilter),
		))
	}

	return states
}

func convertToPersistenceDomainFilter(domainFilter DomainFilter) *types.DomainFilter {
	domainIDs := make([]string, 0, len(domainFilter.DomainIDs))
	for domainID := range domainFilter.DomainIDs {
		domainIDs = append(domainIDs, domainID)
	}

	return &types.DomainFilter{
		DomainIDs:    domainIDs,
		ReverseMatch: domainFilter.ReverseMatch,
	}
}

func convertFromPersistenceDomainFilter(domainFilter *types.DomainFilter) DomainFilter {
	domainIDs := make(map[string]struct{})
	for _, domainID := range domainFilter.DomainIDs {
		domainIDs[domainID] = struct{}{}
	}

	return NewDomainFilter(domainIDs, domainFilter.GetReverseMatch())
}

func validateProcessingQueueStates(pStates []*types.ProcessingQueueState, ackLevel interface{}) bool {
	if len(pStates) == 0 {
		return false
	}

	minAckLevel := pStates[0].GetAckLevel()
	for _, pState := range pStates {
		minAckLevel = min(minAckLevel, pState.GetAckLevel())
	}

	switch ackLevel := ackLevel.(type) {
	case int64:
		return minAckLevel == ackLevel
	case time.Time:
		return minAckLevel == ackLevel.UnixNano()
	default:
		return false
	}
}
