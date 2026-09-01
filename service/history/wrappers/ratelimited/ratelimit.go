package ratelimited

func (h *historyHandler) allowWfID(domainUUID, workflowID string) bool {
	return h.workflowIDCache.AllowExternal(domainUUID, workflowID)
}
