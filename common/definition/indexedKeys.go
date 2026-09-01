package definition

import "github.com/uber/cadence/common/types"

// valid indexed fields on ES
const (
	DomainID               = "DomainID"
	WorkflowID             = "WorkflowID"
	RunID                  = "RunID"
	WorkflowType           = "WorkflowType"
	StartTime              = "StartTime"
	ExecutionTime          = "ExecutionTime"
	CloseTime              = "CloseTime"
	CloseStatus            = "CloseStatus"
	HistoryLength          = "HistoryLength"
	Encoding               = "Encoding"
	KafkaKey               = "KafkaKey"
	BinaryChecksums        = "BinaryChecksums"
	TaskList               = "TaskList"
	ClusterAttributeScope  = "ClusterAttributeScope"
	ClusterAttributeName   = "ClusterAttributeName"
	IsCron                 = "IsCron"
	NumClusters            = "NumClusters"
	UpdateTime             = "UpdateTime"
	CronSchedule           = "CronSchedule"
	ExecutionStatus        = "ExecutionStatus"
	ScheduledExecutionTime = "ScheduledExecutionTime"
	CustomDomain           = "CustomDomain" // to support batch workflow
	Operator               = "Operator"     // to support batch workflow

	// Schedule search attributes set on target workflows started by the scheduler.
	CadenceScheduleID         = "CadenceScheduleID"
	CadenceScheduleTime       = "CadenceScheduleTime"
	CadenceScheduleIsBackfill = "CadenceScheduleIsBackfill"
	// CadenceScheduleBackfillID is set on target workflows started by a schedule
	// backfill, keyed by BackfillScheduleRequest.backfill_id (keyword). The frontend
	// assigns a UUID when the client omits the id.
	CadenceScheduleBackfillID = "CadenceScheduleBackfillID"

	// Schedule search attributes set on the scheduler workflow itself (used by ListSchedules).
	CadenceScheduleState        = "CadenceScheduleState"
	CadenceScheduleCron         = "CadenceScheduleCron"
	CadenceScheduleWorkflowType = "CadenceScheduleWorkflowType"

	CustomStringField    = "CustomStringField"
	CustomKeywordField   = "CustomKeywordField"
	CustomIntField       = "CustomIntField"
	CustomBoolField      = "CustomBoolField"
	CustomDoubleField    = "CustomDoubleField"
	CustomDatetimeField  = "CustomDatetimeField"
	CadenceChangeVersion = "CadenceChangeVersion"
)

const (
	// Memo is valid non-indexed fields on ES
	Memo = "Memo"
	// Attr is prefix of custom search attributes
	Attr = "Attr"
	// HeaderFormat is the format of context headers in search attributes
	HeaderFormat = "Header_%s"
)

// defaultIndexedKeys defines all searchable keys
var defaultIndexedKeys = createDefaultIndexedKeys()

func createDefaultIndexedKeys() map[string]interface{} {
	defaultIndexedKeys := map[string]interface{}{
		CustomStringField:    types.IndexedValueTypeString,
		CustomKeywordField:   types.IndexedValueTypeKeyword,
		CustomIntField:       types.IndexedValueTypeInt,
		CustomBoolField:      types.IndexedValueTypeBool,
		CustomDoubleField:    types.IndexedValueTypeDouble,
		CustomDatetimeField:  types.IndexedValueTypeDatetime,
		CadenceChangeVersion: types.IndexedValueTypeKeyword,
		BinaryChecksums:      types.IndexedValueTypeKeyword,
		CustomDomain:         types.IndexedValueTypeString,
		Operator:             types.IndexedValueTypeString,
		// Schedule search attributes are set by the scheduler workflow/activity via
		// UpsertSearchAttributes and StartWorkflow
		CadenceScheduleID:           types.IndexedValueTypeKeyword,
		CadenceScheduleTime:         types.IndexedValueTypeDatetime,
		CadenceScheduleIsBackfill:   types.IndexedValueTypeBool,
		CadenceScheduleState:        types.IndexedValueTypeKeyword,
		CadenceScheduleCron:         types.IndexedValueTypeKeyword,
		CadenceScheduleWorkflowType: types.IndexedValueTypeKeyword,
		CadenceScheduleBackfillID:   types.IndexedValueTypeKeyword,
	}
	for k, v := range systemIndexedKeys {
		defaultIndexedKeys[k] = v
	}
	return defaultIndexedKeys
}

// GetDefaultIndexedKeys return default valid indexed keys
func GetDefaultIndexedKeys() map[string]interface{} {
	return defaultIndexedKeys
}

// systemIndexedKeys is Cadence created visibility keys
var systemIndexedKeys = map[string]interface{}{
	DomainID:               types.IndexedValueTypeKeyword,
	WorkflowID:             types.IndexedValueTypeKeyword,
	RunID:                  types.IndexedValueTypeKeyword,
	WorkflowType:           types.IndexedValueTypeKeyword,
	StartTime:              types.IndexedValueTypeInt,
	ExecutionTime:          types.IndexedValueTypeInt,
	CloseTime:              types.IndexedValueTypeInt,
	CloseStatus:            types.IndexedValueTypeInt,
	HistoryLength:          types.IndexedValueTypeInt,
	TaskList:               types.IndexedValueTypeKeyword,
	IsCron:                 types.IndexedValueTypeBool,
	NumClusters:            types.IndexedValueTypeInt,
	UpdateTime:             types.IndexedValueTypeInt,
	CronSchedule:           types.IndexedValueTypeKeyword,
	ExecutionStatus:        types.IndexedValueTypeInt,
	ScheduledExecutionTime: types.IndexedValueTypeInt,
	ClusterAttributeScope:  types.IndexedValueTypeKeyword,
	ClusterAttributeName:   types.IndexedValueTypeKeyword,
}

// IsSystemIndexedKey return true is key is system added
func IsSystemIndexedKey(key string) bool {
	_, ok := systemIndexedKeys[key]
	return ok
}

// IsSystemBoolKey return true is key is system added bool key
func IsSystemBoolKey(key string) bool {
	return systemIndexedKeys[key] == types.IndexedValueTypeBool
}
