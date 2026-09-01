package persistence

import (
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

type (
	// DynamicConfiguration represents dynamic configuration for persistence layer
	DynamicConfiguration struct {
		EnableWorkflowTimerTaskCleanup           dynamicproperties.BoolPropertyFn
		WorkflowTimerTaskCleanupMinTTL           dynamicproperties.DurationPropertyFn
		EnableSQLAsyncTransaction                dynamicproperties.BoolPropertyFn
		EnableCassandraAllConsistencyLevelDelete dynamicproperties.BoolPropertyFn
		EnableShardIDMetrics                     dynamicproperties.BoolPropertyFn
		EnableHistoryTaskDualWriteMode           dynamicproperties.BoolPropertyFn
		ReadNoSQLHistoryTaskFromDataBlob         dynamicproperties.BoolPropertyFn
		ReadNoSQLShardFromDataBlob               dynamicproperties.BoolPropertyFn
		SerializationEncoding                    dynamicproperties.StringPropertyFn
		DomainAuditLogTTL                        dynamicproperties.DurationPropertyFnWithDomainIDFilter
		HistoryNodeDeleteBatchSize               dynamicproperties.IntPropertyFn
		RateLimiterBypassCallerTypes             dynamicproperties.ListPropertyFn
	}
)

// NewDynamicConfiguration returns new config with default values
func NewDynamicConfiguration(dc *dynamicconfig.Collection) *DynamicConfiguration {
	return &DynamicConfiguration{
		EnableWorkflowTimerTaskCleanup:           dc.GetBoolProperty(dynamicproperties.EnableWorkflowTimerTaskCleanup),
		WorkflowTimerTaskCleanupMinTTL:           dc.GetDurationProperty(dynamicproperties.WorkflowTimerTaskCleanupMinTTL),
		EnableSQLAsyncTransaction:                dc.GetBoolProperty(dynamicproperties.EnableSQLAsyncTransaction),
		EnableCassandraAllConsistencyLevelDelete: dc.GetBoolProperty(dynamicproperties.EnableCassandraAllConsistencyLevelDelete),
		EnableShardIDMetrics:                     dc.GetBoolProperty(dynamicproperties.EnableShardIDMetrics),
		EnableHistoryTaskDualWriteMode:           dc.GetBoolProperty(dynamicproperties.EnableNoSQLHistoryTaskDualWriteMode),
		ReadNoSQLHistoryTaskFromDataBlob:         dc.GetBoolProperty(dynamicproperties.ReadNoSQLHistoryTaskFromDataBlob),
		ReadNoSQLShardFromDataBlob:               dc.GetBoolProperty(dynamicproperties.ReadNoSQLShardFromDataBlob),
		SerializationEncoding:                    dc.GetStringProperty(dynamicproperties.SerializationEncoding),
		DomainAuditLogTTL:                        dc.GetDurationPropertyFilteredByDomainID(dynamicproperties.DomainAuditLogTTL),
		HistoryNodeDeleteBatchSize:               dc.GetIntProperty(dynamicproperties.HistoryNodeDeleteBatchSize),
		RateLimiterBypassCallerTypes:             dc.GetListProperty(dynamicproperties.RateLimiterBypassCallerTypes),
	}
}
