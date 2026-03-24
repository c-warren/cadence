// Copyright (c) 2017-2021 Uber Technologies, Inc.
// Portions of the Software are attributed to Copyright (c) 2021 Temporal Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package engineimpl

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	hcommon "github.com/uber/cadence/service/history/common"
	"github.com/uber/cadence/service/history/queuev2"
)

func (e *historyEngineImpl) registerDomainFailoverCallback() {

	// NOTE: READ BEFORE MODIFICATION
	//
	// Tasks, e.g. transfer tasks and timer tasks, are created when holding the shard lock
	// meaning tasks -> release of shard lock
	//
	// Domain change notification follows the following steps, order matters
	// 1. lock all task processing.
	// 2. domain changes visible to everyone (Note: lock of task processing prevents task processing logic seeing the domain changes).
	// 3. failover min and max task levels are calculated, then update to shard.
	// 4. failover start & task processing unlock & shard domain version notification update. (order does not matter for this discussion)
	//
	// The above guarantees that task created during the failover will be processed.
	// If the task is created after domain change:
	// 		then active processor will handle it. (simple case)
	// If the task is created before domain change:
	//		task -> release of shard lock
	//		failover min / max task levels calculated & updated to shard (using shard lock) -> failover start
	// above 2 guarantees that failover start is after persistence of the task.

	initialNotificationVersion := e.shard.GetDomainNotificationVersion()

	catchUpFn := func(domainCache cache.DomainCache, prepareCallback cache.PrepareCallbackFn, callback cache.CallbackFn) {
		// this section is trying to make the shard catch up with domain changes
		domains := cache.DomainCacheEntries{}
		for _, domain := range domainCache.GetAllDomain() {
			domains = append(domains, domain)
		}
		// we must notify the change in an ordered fashion
		// since history shard has to update the shard info
		// with the domain change version.
		sort.Sort(domains)

		var updatedEntries []*cache.DomainCacheEntry
		var domainNames []string
		for _, domain := range domains {
			if domain.GetNotificationVersion() >= initialNotificationVersion {
				updatedEntries = append(updatedEntries, domain)
				domainNames = append(domainNames, domain.GetInfo().Name)
			}
		}
		if len(updatedEntries) > 0 {
			e.logger.Info("catchUpFn calling prepareCallback and callback", tag.Dynamic("domain-names", domainNames))
			prepareCallback()
			callback(updatedEntries)
		}
	}

	// first set the failover callback
	e.shard.GetDomainCache().RegisterDomainChangeCallback(
		createShardNameFromShardID(e.shard.GetShardID()),
		catchUpFn,
		e.lockTaskProcessingForDomainUpdate,
		e.domainChangeCB,
	)
}

func (e *historyEngineImpl) domainChangeCB(nextDomains []*cache.DomainCacheEntry) {
	defer func() {
		e.unlockProcessingForDomainUpdate()
	}()

	if len(nextDomains) == 0 {
		return
	}

	shardNotificationVersion := e.shard.GetDomainNotificationVersion()
	failoverActivePassiveDomainIDs := map[string]struct{}{}
	failoverActiveActiveDomainIDs := map[string]struct{}{}

	for _, nextDomain := range nextDomains {
		domainFailoverNotificationVersion := nextDomain.GetFailoverNotificationVersion()
		domainActiveCluster := nextDomain.GetReplicationConfig().ActiveClusterName

		if !nextDomain.IsGlobalDomain() {
			continue
		}

		if !nextDomain.GetReplicationConfig().IsActiveActive() &&
			domainFailoverNotificationVersion >= shardNotificationVersion &&
			domainActiveCluster == e.currentClusterName {
			failoverActivePassiveDomainIDs[nextDomain.GetInfo().ID] = struct{}{}
		}
		if nextDomain.GetReplicationConfig().IsActiveActive() &&
			domainFailoverNotificationVersion >= shardNotificationVersion &&
			nextDomain.IsActiveIn(e.currentClusterName) {
			failoverActiveActiveDomainIDs[nextDomain.GetInfo().ID] = struct{}{}
		}
	}

	if len(failoverActivePassiveDomainIDs) > 0 {
		e.logger.Info("Domain Failover Start.", tag.WorkflowDomainIDs(failoverActivePassiveDomainIDs))

		// Failover queues are not created for active-active domains. Will revisit after new queue framework implementation.
		for _, processor := range e.queueProcessors {
			processor.FailoverDomain(failoverActivePassiveDomainIDs)
		}
	}

	if len(failoverActiveActiveDomainIDs) > 0 {
		e.logger.Info("Active-Active Domain updated", tag.WorkflowDomainIDs(failoverActiveActiveDomainIDs))

		// For active-active domains, we need to process DLQ for each cluster attribute
		// that is now active on the current cluster
		e.processActiveActiveDomainFailover(nextDomains, failoverActiveActiveDomainIDs)
	}

	// Notify queues for any domain update. (active-passive and active-active)
	if len(failoverActivePassiveDomainIDs) > 0 || len(failoverActiveActiveDomainIDs) > 0 {
		e.notifyQueues()
	}

	failoverMarkerTasks := e.generateGracefulFailoverTasksForDomainUpdateCallback(shardNotificationVersion, nextDomains)

	// This is a debug metric
	e.metricsClient.IncCounter(metrics.FailoverMarkerScope, metrics.HistoryFailoverCallbackCount)
	if len(failoverMarkerTasks) > 0 {
		if err := e.shard.ReplicateFailoverMarkers(
			context.Background(),
			failoverMarkerTasks,
		); err != nil {
			e.logger.Error("Failed to insert failover marker to replication queue.", tag.Error(err))
			e.metricsClient.IncCounter(metrics.FailoverMarkerScope, metrics.FailoverMarkerInsertFailure)
			// fail this failover callback and it retries on next domain cache refresh
			return
		}
	}

	//nolint:errcheck
	e.shard.UpdateDomainNotificationVersion(nextDomains[len(nextDomains)-1].GetNotificationVersion() + 1)
}

func (e *historyEngineImpl) notifyQueues() {
	now := e.shard.GetTimeSource().Now()
	// the fake tasks will not be actually used, we just need to make sure
	// its length > 0 and has correct timestamp, to trigger a db scan
	fakeDecisionTask := []persistence.Task{&persistence.DecisionTask{}}
	fakeDecisionTimeoutTask := []persistence.Task{&persistence.DecisionTimeoutTask{TaskData: persistence.TaskData{VisibilityTimestamp: now}}}
	transferProcessor, ok := e.queueProcessors[persistence.HistoryTaskCategoryTransfer]
	if !ok {
		e.logger.Error("transfer processor not found")
		return
	}
	transferProcessor.NotifyNewTask(e.currentClusterName, &hcommon.NotifyTaskInfo{Tasks: fakeDecisionTask})
	timerProcessor, ok := e.queueProcessors[persistence.HistoryTaskCategoryTimer]
	if !ok {
		e.logger.Error("timer processor not found")
		return
	}
	timerProcessor.NotifyNewTask(e.currentClusterName, &hcommon.NotifyTaskInfo{Tasks: fakeDecisionTimeoutTask})
}

func (e *historyEngineImpl) generateGracefulFailoverTasksForDomainUpdateCallback(shardNotificationVersion int64, nextDomains []*cache.DomainCacheEntry) []*persistence.FailoverMarkerTask {
	// handle graceful failover on active to passive
	// make sure task processor failover the domain before inserting the failover marker
	failoverMarkerTasks := []*persistence.FailoverMarkerTask{}
	for _, nextDomain := range nextDomains {
		if nextDomain.GetReplicationConfig().IsActiveActive() {
			// Currently it's unclear whether graceful failover is working for active-passive domains. We don't use it in practice.
			// Don't try to make it work for active-active domains until we determine we need it.
			// We may potentially retire existing graceful failover implementation and provide "sync replication" instead.
			continue
		}
		domainFailoverNotificationVersion := nextDomain.GetFailoverNotificationVersion()
		domainActiveCluster := nextDomain.GetReplicationConfig().ActiveClusterName
		previousFailoverVersion := nextDomain.GetPreviousFailoverVersion()
		previousClusterName, err := e.clusterMetadata.ClusterNameForFailoverVersion(previousFailoverVersion)
		if err != nil && previousFailoverVersion != constants.InitialPreviousFailoverVersion {
			e.logger.Error("Failed to handle graceful failover", tag.WorkflowDomainID(nextDomain.GetInfo().ID), tag.Error(err))
			continue
		}

		if nextDomain.IsGlobalDomain() &&
			domainFailoverNotificationVersion >= shardNotificationVersion &&
			domainActiveCluster != e.currentClusterName &&
			previousFailoverVersion != constants.InitialPreviousFailoverVersion &&
			previousClusterName == e.currentClusterName {
			// the visibility timestamp will be set in shard context
			failoverMarkerTasks = append(failoverMarkerTasks, &persistence.FailoverMarkerTask{
				TaskData: persistence.TaskData{
					Version: nextDomain.GetFailoverVersion(),
				},
				DomainID: nextDomain.GetInfo().ID,
			})
			// This is a debug metric
			e.metricsClient.IncCounter(metrics.FailoverMarkerScope, metrics.FailoverMarkerCallbackCount)
		}
	}
	return failoverMarkerTasks
}

func (e *historyEngineImpl) processActiveActiveDomainFailover(
	nextDomains []*cache.DomainCacheEntry,
	failoverActiveActiveDomainIDs map[string]struct{},
) {
	// Group domains by cluster attribute to process DLQ efficiently
	// Map: cluster attribute key (scope.name) -> domain IDs
	clusterAttrToDomains := make(map[string]map[string]struct{})

	for _, domain := range nextDomains {
		if _, ok := failoverActiveActiveDomainIDs[domain.GetInfo().ID]; !ok {
			continue
		}

		// Extract cluster attributes that are active on the current cluster
		replicationConfig := domain.GetReplicationConfig()
		if replicationConfig.ActiveClusters == nil || replicationConfig.ActiveClusters.AttributeScopes == nil {
			continue
		}

		for scopeName, scope := range replicationConfig.ActiveClusters.AttributeScopes {
			for attrName, clusterInfo := range scope.ClusterAttributes {
				// Only process cluster attributes that are active on the current cluster
				if clusterInfo.ActiveClusterName == e.currentClusterName {
					key := scopeName + "." + attrName
					if _, exists := clusterAttrToDomains[key]; !exists {
						clusterAttrToDomains[key] = make(map[string]struct{})
					}
					clusterAttrToDomains[key][domain.GetInfo().ID] = struct{}{}

					e.logger.Info("Active-active domain cluster attribute active on current cluster",
						tag.WorkflowDomainID(domain.GetInfo().ID),
						tag.Dynamic("scope", scopeName),
						tag.Dynamic("name", attrName),
						tag.Dynamic("active_cluster", clusterInfo.ActiveClusterName))
				}
			}
		}
	}

	// Process DLQ for each cluster attribute
	for clusterAttrKey, domainIDs := range clusterAttrToDomains {
		// Parse the key back to scope and name
		parts := splitOnFirst(clusterAttrKey, ".")
		if len(parts) != 2 {
			e.logger.Error("Invalid cluster attribute key", tag.Dynamic("key", clusterAttrKey))
			continue
		}

		scope := parts[0]
		name := parts[1]

		e.logger.Info("Processing DLQ for active-active cluster attribute",
			tag.Dynamic("scope", scope),
			tag.Dynamic("name", name),
			tag.Dynamic("domain_count", len(domainIDs)))

		// Create cluster attribute key
		clusterAttr := &queuev2.ClusterAttributeKey{
			Scope: scope,
			Name:  name,
		}

		// Call FailoverDomainWithClusterAttribute for each processor
		for _, processor := range e.queueProcessors {
			// Check if processor is a queuev2.Queue
			if qv2Processor, ok := processor.(queuev2.Queue); ok {
				qv2Processor.FailoverDomainWithClusterAttribute(domainIDs, clusterAttr)
			} else {
				// Fallback to regular FailoverDomain for processors that don't support cluster attributes
				e.logger.Warn("Processor does not support cluster attribute failover, falling back to regular failover",
					tag.Dynamic("processor_type", fmt.Sprintf("%T", processor)))
				processor.FailoverDomain(domainIDs)
			}
		}
	}
}

// splitOnFirst splits a string on the first occurrence of sep
func splitOnFirst(s, sep string) []string {
	parts := strings.SplitN(s, sep, 2)
	return parts
}

func (e *historyEngineImpl) lockTaskProcessingForDomainUpdate() {
	e.logger.Debug("Locking processing for domain update")
	for _, processor := range e.queueProcessors {
		processor.LockTaskProcessing()
	}
}

func (e *historyEngineImpl) unlockProcessingForDomainUpdate() {
	e.logger.Debug("Unlocking processing for failover")
	for _, processor := range e.queueProcessors {
		processor.UnlockTaskProcessing()
	}
}

func (e *historyEngineImpl) failoverPredicate(shardNotificationVersion int64, nextDomain *cache.DomainCacheEntry, action func()) {
	domainFailoverNotificationVersion := nextDomain.GetFailoverNotificationVersion()
	domainActiveCluster := nextDomain.GetReplicationConfig().ActiveClusterName

	if nextDomain.IsGlobalDomain() &&
		!nextDomain.GetReplicationConfig().IsActiveActive() &&
		domainFailoverNotificationVersion >= shardNotificationVersion &&
		domainActiveCluster == e.currentClusterName {
		action()
	}
}
