package elasticsearch

import "github.com/uber/cadence/.gen/go/indexer"

// All legal fields allowed in elastic search index
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
	Memo                   = "Memo"
	Encoding               = "Encoding"
	TaskList               = "TaskList"
	IsCron                 = "IsCron"
	NumClusters            = "NumClusters"
	ClusterAttributeScope  = "ClusterAttributeScope"
	ClusterAttributeName   = "ClusterAttributeName"
	VisibilityOperation    = "VisibilityOperation"
	UpdateTime             = "UpdateTime"
	ShardID                = "ShardID"
	CronSchedule           = "CronSchedule"
	ExecutionStatus        = "ExecutionStatus"
	ScheduledExecutionTime = "ScheduledExecutionTime"
)

// Supported field types
var (
	FieldTypeString = indexer.FieldTypeString
	FieldTypeInt    = indexer.FieldTypeInt
	FieldTypeBool   = indexer.FieldTypeBool
	FieldTypeBinary = indexer.FieldTypeBinary
)
