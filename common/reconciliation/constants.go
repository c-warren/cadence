package reconciliation

// Execution fixer workflow relates

const (
	CheckDataCorruptionWorkflowType                 = "check-data-corruption-workflow"
	CheckDataCorruptionWorkflowTaskList             = "check-data-corruption-workflow-tl"
	CheckDataCorruptionWorkflowSignalName           = "check-data-corruption-workflow-signal"
	CheckDataCorruptionWorkflowID                   = "check-data-corruption-workflow-id"
	CheckDataCorruptionWorkflowTimeoutInSeconds     = 24 * 60 * 60
	CheckDataCorruptionWorkflowTaskTimeoutInSeconds = 60
)
