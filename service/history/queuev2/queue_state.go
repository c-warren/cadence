package queuev2

import "github.com/uber/cadence/common/persistence"

type QueueState struct {
	VirtualQueueStates    map[int64][]VirtualSliceState
	ExclusiveMaxReadLevel persistence.HistoryTaskKey
}

type VirtualSliceState struct {
	Range     Range
	Predicate Predicate
}

func (s *VirtualSliceState) IsEmpty() bool {
	return s.Range.IsEmpty() || s.Predicate.IsEmpty()
}

func (s *VirtualSliceState) Contains(task persistence.Task) bool {
	return s.Range.Contains(task.GetTaskKey()) && s.Predicate.Check(task)
}

func (s *VirtualSliceState) TrySplitByTaskKey(taskKey persistence.HistoryTaskKey) (VirtualSliceState, VirtualSliceState, bool) {
	if !s.Range.CanSplitByTaskKey(taskKey) {
		return VirtualSliceState{}, VirtualSliceState{}, false
	}

	return VirtualSliceState{
			Range:     Range{InclusiveMinTaskKey: s.Range.InclusiveMinTaskKey, ExclusiveMaxTaskKey: taskKey},
			Predicate: s.Predicate,
		}, VirtualSliceState{
			Range:     Range{InclusiveMinTaskKey: taskKey, ExclusiveMaxTaskKey: s.Range.ExclusiveMaxTaskKey},
			Predicate: s.Predicate,
		}, true
}

func (s *VirtualSliceState) TrySplitByPredicate(predicate Predicate) (VirtualSliceState, VirtualSliceState, bool) {
	if predicate.Equals(&universalPredicate{}) || predicate.Equals(&emptyPredicate{}) || predicate.Equals(s.Predicate) {
		return VirtualSliceState{}, VirtualSliceState{}, false
	}
	return VirtualSliceState{
			Range:     s.Range,
			Predicate: And(s.Predicate, predicate),
		}, VirtualSliceState{
			Range:     s.Range,
			Predicate: And(s.Predicate, Not(predicate)),
		}, true
}
