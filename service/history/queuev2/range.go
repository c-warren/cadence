package queuev2

import (
	"github.com/uber/cadence/common/persistence"
)

type Range struct {
	InclusiveMinTaskKey persistence.HistoryTaskKey
	ExclusiveMaxTaskKey persistence.HistoryTaskKey
}

func (r *Range) IsEmpty() bool {
	return r.InclusiveMinTaskKey.Compare(r.ExclusiveMaxTaskKey) >= 0
}

func (r *Range) Contains(taskKey persistence.HistoryTaskKey) bool {
	return taskKey.Compare(r.InclusiveMinTaskKey) >= 0 && taskKey.Compare(r.ExclusiveMaxTaskKey) < 0
}

func (r *Range) ContainsRange(other Range) bool {
	return r.InclusiveMinTaskKey.Compare(other.InclusiveMinTaskKey) <= 0 && r.ExclusiveMaxTaskKey.Compare(other.ExclusiveMaxTaskKey) >= 0
}

func (r *Range) CanMerge(other Range) bool {
	return r.InclusiveMinTaskKey.Compare(other.ExclusiveMaxTaskKey) <= 0 && r.ExclusiveMaxTaskKey.Compare(other.InclusiveMinTaskKey) >= 0
}

func (r *Range) CanSplitByTaskKey(taskKey persistence.HistoryTaskKey) bool {
	return taskKey.Compare(r.InclusiveMinTaskKey) > 0 && taskKey.Compare(r.ExclusiveMaxTaskKey) < 0
}
