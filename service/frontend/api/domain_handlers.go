// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber/cadence/common/domain/audit"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/frontend/validate"
)

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandler) RegisterDomain(ctx context.Context, registerRequest *types.RegisterDomainRequest) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateRegisterDomainRequest(ctx, registerRequest); err != nil {
		return err
	}
	return wh.domainHandler.RegisterDomain(ctx, registerRequest)
}

// ListDomains returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) ListDomains(
	ctx context.Context,
	listRequest *types.ListDomainsRequest,
) (response *types.ListDomainsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	return wh.domainHandler.ListDomains(ctx, listRequest)
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) DescribeDomain(
	ctx context.Context,
	describeRequest *types.DescribeDomainRequest,
) (response *types.DescribeDomainResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateDescribeDomainRequest(ctx, describeRequest); err != nil {
		return nil, err
	}
	resp, err := wh.domainHandler.DescribeDomain(ctx, describeRequest)
	if err != nil {
		return nil, err
	}

	if resp.GetFailoverInfo() != nil && resp.GetFailoverInfo().GetFailoverExpireTimestamp() > 0 {
		// fetch ongoing failover info from history service
		failoverResp, err := wh.GetHistoryClient().GetFailoverInfo(ctx, &types.GetFailoverInfoRequest{
			DomainID: resp.GetDomainInfo().UUID,
		})
		if err != nil {
			// despite the error from history, return describe domain response
			wh.GetLogger().Error(
				fmt.Sprintf("Failed to get failover info for domain %s", resp.DomainInfo.GetName()),
				tag.Error(err),
			)
			return resp, nil
		}
		resp.FailoverInfo.CompletedShardCount = failoverResp.GetCompletedShardCount()
		resp.FailoverInfo.PendingShards = failoverResp.GetPendingShards()
	}
	return resp, nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandler) UpdateDomain(
	ctx context.Context,
	updateRequest *types.UpdateDomainRequest,
) (resp *types.UpdateDomainResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateUpdateDomainRequest(ctx, updateRequest); err != nil {
		return nil, err
	}

	domainName := updateRequest.GetName()
	logger := wh.GetLogger().WithTags(
		tag.WorkflowDomainName(domainName),
		tag.OperationName("DomainUpdate"))

	isFailover := isFailoverRequest(updateRequest)
	isGraceFailover := isGraceFailoverRequest(updateRequest)
	logger.Info(fmt.Sprintf(
		"Domain Update requested. isFailover: %v, isGraceFailover: %v, Request: %#v.",
		isFailover,
		isGraceFailover,
		updateRequest))

	if isGraceFailover {
		if err := wh.checkOngoingFailover(
			ctx,
			&updateRequest.Name,
		); err != nil {
			logger.Error("Graceful domain failover request failed. Not able to check ongoing failovers.",
				tag.Error(err))
			return nil, err
		}
	}

	// TODO: call remote clusters to verify domain data
	resp, err := wh.domainHandler.UpdateDomain(ctx, updateRequest)
	if err != nil {
		logger.Error("Domain update operation failed.",
			tag.Error(err))
		return nil, err
	}
	logger.Info("Domain update operation succeeded.")
	return resp, nil
}

// DeleteDomain permanently removes a domain record. This operation:
// - Requires domain to be in DEPRECATED status
// - Cannot be performed on domains with running workflows
// - Is irreversible and removes all domain data
func (wh *WorkflowHandler) DeleteDomain(ctx context.Context, deleteRequest *types.DeleteDomainRequest) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateDeleteDomainRequest(ctx, deleteRequest); err != nil {
		return err
	}

	domainName := deleteRequest.GetName()
	resp, err := wh.domainHandler.DescribeDomain(ctx, &types.DescribeDomainRequest{Name: &domainName})
	if err != nil {
		return err
	}

	if *resp.DomainInfo.Status != types.DomainStatusDeprecated {
		return &types.BadRequestError{Message: "Domain is not in a deprecated state."}
	}

	workflowList, err := wh.ListWorkflowExecutions(ctx, &types.ListWorkflowExecutionsRequest{
		Domain: domainName,
	})
	if err != nil {
		return err
	}

	if len(workflowList.Executions) != 0 {
		return &types.BadRequestError{Message: "Domain still have workflow execution history."}
	}

	return wh.domainHandler.DeleteDomain(ctx, deleteRequest)
}

// DeprecateDomain is used to update status of a registered domain to DEleTED. Once the domain is deleted
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandler) DeprecateDomain(ctx context.Context, deprecateRequest *types.DeprecateDomainRequest) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateDeprecateDomainRequest(ctx, deprecateRequest); err != nil {
		return err
	}
	return wh.domainHandler.DeprecateDomain(ctx, deprecateRequest)
}

// FailoverDomain is used to failover a registered domain to different cluster.
func (wh *WorkflowHandler) FailoverDomain(ctx context.Context, failoverRequest *types.FailoverDomainRequest) (*types.FailoverDomainResponse, error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateFailoverDomainRequest(ctx, failoverRequest); err != nil {
		return nil, err
	}

	domainName := failoverRequest.GetDomainName()
	logger := wh.GetLogger().WithTags(
		tag.WorkflowDomainName(domainName),
		tag.OperationName("FailoverDomain"))

	logger.Info(fmt.Sprintf("Failover domain is requested. Request: %#v.", failoverRequest))

	failoverResp, err := wh.domainHandler.FailoverDomain(ctx, failoverRequest)
	if err != nil {
		logger.Error("Failover domain operation failed.",
			tag.Error(err))
		return nil, err
	}

	logger.Info("Failover domain operation succeeded.")
	return failoverResp, nil
}

// ListFailoverHistory returns the failover history for a domain
func (wh *WorkflowHandler) ListFailoverHistory(
	ctx context.Context,
	request *types.ListFailoverHistoryRequest,
) (*types.ListFailoverHistoryResponse, error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	// Extract domain ID from filters
	if request.Filters == nil || request.Filters.DomainID == nil || *request.Filters.DomainID == "" {
		return nil, &types.BadRequestError{Message: "domain_id is required in filters"}
	}

	domainID := *request.Filters.DomainID

	// Set default page size
	pageSize := 5
	if request.Pagination != nil && request.Pagination.PageSize != nil && *request.Pagination.PageSize > 0 {
		pageSize = int(*request.Pagination.PageSize)
	}

	var nextPageToken []byte
	if request.Pagination != nil {
		nextPageToken = request.Pagination.NextPageToken
	}

	// Read from audit log
	readResp, err := wh.GetDomainManager().ReadDomainAuditLog(ctx, &persistence.ReadDomainAuditLogRequest{
		DomainID:      domainID,
		PageSize:      pageSize,
		NextPageToken: nextPageToken,
	})
	if err != nil {
		return nil, err
	}

	events := make([]*types.FailoverEvent, 0, len(readResp.Entries))

	for _, entry := range readResp.Entries {
		// Only include failover operations
		if entry.OperationType != persistence.DomainOperationTypeFailover {
			continue
		}

		// Convert created time to Unix milliseconds
		createdTimeMs := entry.CreatedTime.UnixNano() / int64(1000000)
		eventID := entry.EventID
		failoverType := types.FailoverTypeForce // TODO: Determine from entry

		event := &types.FailoverEvent{
			ID:           &eventID,
			CreatedTime:  &createdTimeMs,
			FailoverType: &failoverType,
		}

		// Note: FailoverEvent in List doesn't include detailed cluster failovers
		// Use GetFailoverEvent for full details
		// POC Toggle exists for testing decompression performance in List queries

		// Apply filters (using change summary from comment)
		if shouldIncludeEvent(entry.Comment, request.Filters) {
			events = append(events, event)
		}
	}

	return &types.ListFailoverHistoryResponse{
		FailoverEvents: events,
		NextPageToken:  readResp.NextPageToken,
	}, nil
}

// shouldIncludeEvent filters events based on change summary
func shouldIncludeEvent(
	summaryJSON string,
	filters *types.ListFailoverHistoryRequestFilters,
) bool {
	// Parse change summary from comment
	var summary audit.ChangeSummary
	if err := json.Unmarshal([]byte(summaryJSON), &summary); err != nil {
		// If parsing fails, include the event (fail-open for filtering)
		return true
	}

	// Filter: default cluster only
	if filters.DefaultActiveCluster != nil && *filters.DefaultActiveCluster && !summary.DefaultClusterChanged {
		return false
	}

	// Filter: specific cluster attributes
	if len(filters.Attributes) > 0 {
		if !matchesAttributeFilter(summary.ClusterAttributesChanged, filters.Attributes) {
			return false
		}
	}

	return true
}

// matchesAttributeFilter checks if any changed attributes match the requested filters
func matchesAttributeFilter(
	changed []*audit.ClusterAttributeRef,
	requested []*types.ClusterAttribute,
) bool {
	if len(changed) == 0 {
		return false
	}

	for _, req := range requested {
		if req == nil {
			continue
		}
		for _, chg := range changed {
			if chg.Scope == req.Scope && chg.Name == req.Name {
				return true
			}
		}
	}
	return false
}

// GetFailoverEvent retrieves detailed information about a specific failover event
func (wh *WorkflowHandler) GetFailoverEvent(
	ctx context.Context,
	request *types.GetFailoverEventRequest,
) (*types.GetFailoverEventResponse, error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	// Validate request
	if request.DomainID == nil || *request.DomainID == "" ||
		request.FailoverEventID == nil || *request.FailoverEventID == "" ||
		request.CreatedTime == nil {
		return nil, &types.BadRequestError{
			Message: "domain_id, failover_event_id, and created_time are required",
		}
	}

	logger := wh.GetLogger()
	logger.Info(fmt.Sprintf("GetFailoverEvent request received: domain_id=%s, event_id=%s, created_time=%d",
		*request.DomainID, *request.FailoverEventID, *request.CreatedTime))

	// Convert Unix milliseconds to time.Time
	logger.Info(fmt.Sprintf("DEBUG: Converting timestamp from Unix milliseconds: %d", *request.CreatedTime))

	createdTime := time.Unix(0, *request.CreatedTime*int64(1000000)) // Convert ms to ns

	logger.Info(fmt.Sprintf("DEBUG: Converted timestamp successfully: unix_nanos=%d, formatted=%s",
		createdTime.UnixNano(), createdTime.Format(time.RFC3339Nano)))

	// Get specific audit log entry
	logger.Info(fmt.Sprintf("DEBUG: Querying for audit log entry: domain_id=%s, event_id=%s",
		*request.DomainID, *request.FailoverEventID),
		tag.Timestamp(createdTime))

	entryResp, err := wh.GetDomainManager().GetDomainAuditLogEntry(ctx, &persistence.GetDomainAuditLogEntryRequest{
		DomainID:    *request.DomainID,
		EventID:     *request.FailoverEventID,
		CreatedTime: createdTime,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to get domain audit log entry: domain_id=%s, event_id=%s",
			*request.DomainID, *request.FailoverEventID), tag.Error(err))
		return nil, err
	}

	if entryResp == nil {
		logger.Warn(fmt.Sprintf("GetDomainAuditLogEntry returned nil response: domain_id=%s, event_id=%s",
			*request.DomainID, *request.FailoverEventID))
		return &types.GetFailoverEventResponse{}, nil
	}

	if entryResp.Entry == nil {
		logger.Warn(fmt.Sprintf("GetDomainAuditLogEntry returned nil entry: domain_id=%s, event_id=%s",
			*request.DomainID, *request.FailoverEventID))
		return &types.GetFailoverEventResponse{}, nil
	}

	entry := entryResp.Entry

	logger.Info(fmt.Sprintf("DEBUG: Retrieved audit log entry successfully: domain_id=%s, event_id=%s, created_time=%s, state_before_size=%d, state_after_size=%d, state_before_encoding=%s, state_after_encoding=%s",
		entry.DomainID, entry.EventID, entry.CreatedTime.Format(time.RFC3339),
		len(entry.StateBefore), len(entry.StateAfter),
		entry.StateBeforeEncoding, entry.StateAfterEncoding))

	// Check if states are empty
	if len(entry.StateBefore) == 0 {
		logger.Warn("state_before is empty")
	}
	if len(entry.StateAfter) == 0 {
		logger.Warn("state_after is empty")
	}

	// Decompress both states
	logger.Info("DEBUG: Decompressing state_before")
	before, err := audit.DecompressAndDeserialize(entry.StateBefore)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to decompress state_before: size=%d", len(entry.StateBefore)), tag.Error(err))
		return nil, &types.InternalServiceError{Message: "Failed to decompress domain state"}
	}
	logger.Info(fmt.Sprintf("DEBUG: state_before decompressed successfully: has_info=%v, has_config=%v, has_replication_config=%v",
		before.Info != nil, before.Config != nil, before.ReplicationConfig != nil))

	logger.Info("DEBUG: Decompressing state_after")
	after, err := audit.DecompressAndDeserialize(entry.StateAfter)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to decompress state_after: size=%d", len(entry.StateAfter)), tag.Error(err))
		return nil, &types.InternalServiceError{Message: "Failed to decompress domain state"}
	}
	logger.Info(fmt.Sprintf("DEBUG: state_after decompressed successfully: has_info=%v, has_config=%v, has_replication_config=%v",
		after.Info != nil, after.Config != nil, after.ReplicationConfig != nil))

	// Log cluster info for debugging
	if before.ReplicationConfig != nil && after.ReplicationConfig != nil {
		logger.Info(fmt.Sprintf("DEBUG: Cluster info: before_active_cluster=%s, after_active_cluster=%s, before_has_active_clusters=%v, after_has_active_clusters=%v",
			before.ReplicationConfig.ActiveClusterName, after.ReplicationConfig.ActiveClusterName,
			before.ReplicationConfig.ActiveClusters != nil, after.ReplicationConfig.ActiveClusters != nil))
	}

	// Compute detailed cluster failovers
	logger.Info("DEBUG: Computing cluster failovers")
	clusterFailovers, err := audit.ComputeClusterFailovers(before, after)
	if err != nil {
		logger.Error("Failed to compute cluster failovers", tag.Error(err))
		return nil, &types.InternalServiceError{Message: "Failed to compute failover details"}
	}

	logger.Info(fmt.Sprintf("DEBUG: Computed cluster failovers: num_failovers=%d", len(clusterFailovers)))

	// Log each failover for debugging
	for i, failover := range clusterFailovers {
		isDefault := failover.IsDefaultCluster != nil && *failover.IsDefaultCluster
		logger.Info(fmt.Sprintf("DEBUG: Cluster failover detail [%d]: is_default=%v, from_cluster=%s, to_cluster=%s, has_attribute=%v",
			i, isDefault, failover.FromCluster.ActiveClusterName, failover.ToCluster.ActiveClusterName,
			failover.ClusterAttribute != nil))
	}

	logger.Info(fmt.Sprintf("DEBUG: Returning response with %d cluster failovers", len(clusterFailovers)))

	return &types.GetFailoverEventResponse{
		ClusterFailovers: clusterFailovers,
	}, nil
}
