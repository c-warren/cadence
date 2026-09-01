package shardscanner

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/store"
)

// Fixer is used to fix entities in a shard. It is responsible for three things:
// 1. Confirming that each entity it scans is corrupted.
// 2. Attempting to fix any confirmed corrupted executions.
// 3. Recording skipped entities, failed to fix entities and successfully fix entities to durable store.
// 4. Producing a FixReport
type Fixer interface {
	Fix() FixReport
}

type (
	// ShardFixer is a generic fixer which iterates over entities provided by iterator
	// implementations of this fixer have to provided invariant manager and iterator.
	ShardFixer struct {
		ctx              context.Context
		shardID          int
		itr              store.ScanOutputIterator
		skippedWriter    store.ExecutionWriter
		failedWriter     store.ExecutionWriter
		fixedWriter      store.ExecutionWriter
		invariantManager invariant.Manager
		progressReportFn func()
		domainCache      cache.DomainCache
		allowDomain      dynamicproperties.BoolPropertyFnWithDomainFilter
		scope            metrics.Scope
	}
)

// NewFixer constructs a new shard fixer.
func NewFixer(
	ctx context.Context,
	shardID int,
	manager invariant.Manager,
	iterator store.ScanOutputIterator,
	blobstoreClient blobstore.Client,
	blobstoreFlushThreshold int,
	progressReportFn func(),
	domainCache cache.DomainCache,
	allowDomain dynamicproperties.BoolPropertyFnWithDomainFilter,
	scope metrics.Scope,
) *ShardFixer {
	id := uuid.New()

	return &ShardFixer{
		ctx:              ctx,
		shardID:          shardID,
		itr:              iterator,
		skippedWriter:    store.NewBlobstoreWriter(id, store.SkippedExtension, blobstoreClient, blobstoreFlushThreshold),
		failedWriter:     store.NewBlobstoreWriter(id, store.FailedExtension, blobstoreClient, blobstoreFlushThreshold),
		fixedWriter:      store.NewBlobstoreWriter(id, store.FixedExtension, blobstoreClient, blobstoreFlushThreshold),
		invariantManager: manager,
		progressReportFn: progressReportFn,
		domainCache:      domainCache,
		allowDomain:      allowDomain,
		scope:            scope,
	}
}

// Fix scans over all executions in shard and runs invariant fixes per execution.
func (f *ShardFixer) Fix() FixReport {

	result := FixReport{
		ShardID:     f.shardID,
		DomainStats: map[string]*FixStats{},
	}

	for f.itr.HasNext() {
		f.progressReportFn()
		soe, err := f.itr.Next()
		if err != nil {
			result.Result.ControlFlowFailure = &ControlFlowFailure{
				Info:        "blobstore iterator returned error",
				InfoDetails: err.Error(),
			}
			return result
		}

		domainID := soe.Execution.(entity.Entity).GetDomainID()
		domainName, err := f.domainCache.GetDomainName(domainID)
		if err != nil {
			result.Result.ControlFlowFailure = &ControlFlowFailure{
				Info:        "failed to get domain name",
				InfoDetails: err.Error(),
			}
			return result
		}
		if _, ok := result.DomainStats[domainID]; !ok {
			result.DomainStats[domainID] = &FixStats{}
		}

		var fixResult invariant.ManagerFixResult

		if f.allowDomain(domainName) {
			fixResult = f.invariantManager.RunFixes(f.ctx, soe.Execution)
		} else {
			fixResult = invariant.ManagerFixResult{
				FixResultType: invariant.FixResultTypeSkipped,
			}
		}
		result.Stats.EntitiesCount++
		result.DomainStats[domainID].EntitiesCount++
		foe := store.FixOutputEntity{
			Execution: soe.Execution,
			Input:     *soe,
			Result:    fixResult,
		}

		invariantName := ""
		if fixResult.DeterminingInvariantName != nil {
			invariantName = string(*fixResult.DeterminingInvariantName)
		}

		f.scope.Tagged(
			metrics.DomainTag(domainName),
			metrics.InvariantTypeTag(invariantName),
			metrics.ShardScannerFixResult(string(fixResult.FixResultType)),
		).IncCounter(metrics.ShardScannerFix)

		switch fixResult.FixResultType {
		case invariant.FixResultTypeFixed:
			if err := f.fixedWriter.Add(foe); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for fixed execution fix",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.FixedCount++
			result.DomainStats[domainID].FixedCount++
		case invariant.FixResultTypeSkipped:
			if err := f.skippedWriter.Add(foe); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for skipped execution fix",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.SkippedCount++
			result.DomainStats[domainID].SkippedCount++
		case invariant.FixResultTypeFailed:
			if err := f.failedWriter.Add(foe); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for failed execution fix",
					InfoDetails: err.Error(),
				}
				return result
			}
			result.Stats.FailedCount++
			result.DomainStats[domainID].FailedCount++
		default:
			panic(fmt.Sprintf("unknown FixResultType: %v", fixResult.FixResultType))
		}
	}
	if err := f.fixedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for fixed execution fixes",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := f.skippedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for skipped execution fixes",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := f.failedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for failed execution fixes",
			InfoDetails: err.Error(),
		}
		return result
	}
	result.Result.ShardFixKeys = &FixKeys{
		Fixed:   f.fixedWriter.FlushedKeys(),
		Failed:  f.failedWriter.FlushedKeys(),
		Skipped: f.skippedWriter.FlushedKeys(),
	}
	return result
}
