package archiver

import (
	"context"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

type (
	// ArchiveHistoryRequest is request to Archive workflow history
	ArchiveHistoryRequest struct {
		ShardID              int
		DomainID             string
		DomainName           string
		WorkflowID           string
		RunID                string
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
	}

	// GetHistoryRequest is the request to Get archived history
	GetHistoryRequest struct {
		DomainID             string
		WorkflowID           string
		RunID                string
		CloseFailoverVersion *int64
		NextPageToken        []byte
		PageSize             int
	}

	// GetHistoryResponse is the response of Get archived history
	GetHistoryResponse struct {
		HistoryBatches []*types.History
		NextPageToken  []byte
	}

	// HistoryBootstrapContainer contains components needed by all history Archiver implementations
	HistoryBootstrapContainer struct {
		HistoryV2Manager  persistence.HistoryManager
		Logger            log.Logger
		MetricsClient     metrics.Client
		ClusterMetadata   cluster.Metadata
		DomainCache       cache.DomainCache
		DynamicCollection *dynamicconfig.Collection
	}

	// HistoryArchiver is used to archive history and read archived history
	HistoryArchiver interface {
		Archive(context.Context, URI, *ArchiveHistoryRequest, ...ArchiveOption) error
		Get(context.Context, URI, *GetHistoryRequest) (*GetHistoryResponse, error)
		ValidateURI(URI) error
	}

	// VisibilityBootstrapContainer contains components needed by all visibility Archiver implementations
	VisibilityBootstrapContainer struct {
		Logger            log.Logger
		MetricsClient     metrics.Client
		ClusterMetadata   cluster.Metadata
		DomainCache       cache.DomainCache
		DynamicCollection *dynamicconfig.Collection
	}

	// ArchiveVisibilityRequest is request to Archive single workflow visibility record
	ArchiveVisibilityRequest struct {
		DomainID           string
		DomainName         string // doesn't need to be archived
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		CloseTimestamp     int64
		CloseStatus        types.WorkflowExecutionCloseStatus
		HistoryLength      int64
		Memo               *types.Memo
		SearchAttributes   map[string]string
		HistoryArchivalURI string
	}

	// QueryVisibilityRequest is the request to query archived visibility records
	QueryVisibilityRequest struct {
		DomainID      string
		PageSize      int
		NextPageToken []byte
		Query         string
	}

	// QueryVisibilityResponse is the response of querying archived visibility records
	QueryVisibilityResponse struct {
		Executions    []*types.WorkflowExecutionInfo
		NextPageToken []byte
	}

	// VisibilityArchiver is used to archive visibility and read archived visibility
	VisibilityArchiver interface {
		Archive(context.Context, URI, *ArchiveVisibilityRequest, ...ArchiveOption) error
		Query(context.Context, URI, *QueryVisibilityRequest) (*QueryVisibilityResponse, error)
		ValidateURI(URI) error
	}
)
