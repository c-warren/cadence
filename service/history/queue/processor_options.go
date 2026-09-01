package queue

import (
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/metrics"
)

type queueProcessorOptions struct {
	BatchSize                            dynamicproperties.IntPropertyFn
	DeleteBatchSize                      dynamicproperties.IntPropertyFn
	MaxPollRPS                           dynamicproperties.IntPropertyFn
	MaxPollInterval                      dynamicproperties.DurationPropertyFn
	MaxPollIntervalJitterCoefficient     dynamicproperties.FloatPropertyFn
	UpdateAckInterval                    dynamicproperties.DurationPropertyFn
	UpdateAckIntervalJitterCoefficient   dynamicproperties.FloatPropertyFn
	RedispatchInterval                   dynamicproperties.DurationPropertyFn
	MaxRedispatchQueueSize               dynamicproperties.IntPropertyFn
	MaxStartJitterInterval               dynamicproperties.DurationPropertyFn
	SplitQueueInterval                   dynamicproperties.DurationPropertyFn
	SplitQueueIntervalJitterCoefficient  dynamicproperties.FloatPropertyFn
	EnableSplit                          dynamicproperties.BoolPropertyFn
	SplitMaxLevel                        dynamicproperties.IntPropertyFn
	EnableRandomSplitByDomainID          dynamicproperties.BoolPropertyFnWithDomainIDFilter
	RandomSplitProbability               dynamicproperties.FloatPropertyFn
	EnablePendingTaskSplitByDomainID     dynamicproperties.BoolPropertyFnWithDomainIDFilter
	PendingTaskSplitThreshold            dynamicproperties.MapPropertyFn
	EnableStuckTaskSplitByDomainID       dynamicproperties.BoolPropertyFnWithDomainIDFilter
	StuckTaskSplitThreshold              dynamicproperties.MapPropertyFn
	SplitLookAheadDurationByDomainID     dynamicproperties.DurationPropertyFnWithDomainIDFilter
	PollBackoffInterval                  dynamicproperties.DurationPropertyFn
	PollBackoffIntervalJitterCoefficient dynamicproperties.FloatPropertyFn
	EnablePersistQueueStates             dynamicproperties.BoolPropertyFn
	EnableLoadQueueStates                dynamicproperties.BoolPropertyFn
	EnableGracefulSyncShutdown           dynamicproperties.BoolPropertyFn
	EnableValidator                      dynamicproperties.BoolPropertyFn
	ValidationInterval                   dynamicproperties.DurationPropertyFn
	// MaxPendingTaskSize is used in cross cluster queue to limit the pending task count
	MaxPendingTaskSize dynamicproperties.IntPropertyFn
	MetricScope        metrics.ScopeIdx
}
