package queue

import (
	"fmt"

	"github.com/uber/cadence/service/history/task"
)

type processingQueueStateImpl struct {
	level        int
	ackLevel     task.Key
	readLevel    task.Key
	maxLevel     task.Key
	domainFilter DomainFilter
}

// NewProcessingQueueState creates a new state instance for processing queue
// readLevel will be set to the same value as ackLevel
func NewProcessingQueueState(
	level int,
	ackLevel task.Key,
	maxLevel task.Key,
	domainFilter DomainFilter,
) ProcessingQueueState {
	return newProcessingQueueState(
		level,
		ackLevel,
		ackLevel,
		maxLevel,
		domainFilter,
	)
}

func newProcessingQueueState(
	level int,
	ackLevel task.Key,
	readLevel task.Key,
	maxLevel task.Key,
	domainFilter DomainFilter,
) *processingQueueStateImpl {
	return &processingQueueStateImpl{
		level:        level,
		ackLevel:     ackLevel,
		readLevel:    readLevel,
		maxLevel:     maxLevel,
		domainFilter: domainFilter,
	}
}

func (s *processingQueueStateImpl) Level() int {
	return s.level
}

func (s *processingQueueStateImpl) MaxLevel() task.Key {
	return s.maxLevel
}

func (s *processingQueueStateImpl) AckLevel() task.Key {
	return s.ackLevel
}

func (s *processingQueueStateImpl) ReadLevel() task.Key {
	return s.readLevel
}

func (s *processingQueueStateImpl) DomainFilter() DomainFilter {
	return s.domainFilter
}

func (s *processingQueueStateImpl) String() string {
	return fmt.Sprintf("&{level: %+v, ackLevel: %+v, readLevel: %+v, maxLevel: %+v, domainFilter: %+v}",
		s.level, s.ackLevel, s.readLevel, s.maxLevel, s.domainFilter,
	)
}

func copyQueueState(state ProcessingQueueState) *processingQueueStateImpl {
	return newProcessingQueueState(
		state.Level(),
		state.AckLevel(),
		state.ReadLevel(),
		state.MaxLevel(),
		state.DomainFilter(),
	)
}
