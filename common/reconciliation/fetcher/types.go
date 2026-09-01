package fetcher

// ExecutionRequest is used to fetch execution from persistence
type ExecutionRequest struct {
	DomainID   string
	WorkflowID string
	RunID      string
	DomainName string
}
