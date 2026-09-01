package archiver

import (
	"errors"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/types"
)

var (
	errEmptyDomainID         = errors.New("DomainID is empty")
	errEmptyDomainName       = errors.New("DomainName is empty")
	errEmptyWorkflowID       = errors.New("WorkflowID is empty")
	errEmptyRunID            = errors.New("RunID is empty")
	errInvalidPageSize       = errors.New("PageSize should be greater than 0")
	errEmptyWorkflowTypeName = errors.New("WorkflowTypeName is empty")
	errEmptyStartTime        = errors.New("StartTimestamp is empty")
	errEmptyCloseTime        = errors.New("CloseTimestamp is empty")
	errEmptyQuery            = errors.New("Query string is empty")
)

// TagLoggerWithArchiveHistoryRequestAndURI tags logger with fields in the archive history request and the URI
func TagLoggerWithArchiveHistoryRequestAndURI(logger log.Logger, request *ArchiveHistoryRequest, URI string) log.Logger {
	return logger.WithTags(
		tag.ShardID(request.ShardID),
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(URI),
	)
}

// TagLoggerWithArchiveVisibilityRequestAndURI tags logger with fields in the archive visibility request and the URI
func TagLoggerWithArchiveVisibilityRequestAndURI(logger log.Logger, request *ArchiveVisibilityRequest, URI string) log.Logger {
	return logger.WithTags(
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchvialRequestWorkflowType(request.WorkflowTypeName),
		tag.ArchivalRequestCloseTimestamp(request.CloseTimestamp),
		tag.ArchivalRequestCloseStatus(request.CloseStatus.String()),
		tag.ArchivalURI(URI),
	)
}

// ValidateHistoryArchiveRequest validates the archive history request
func ValidateHistoryArchiveRequest(request *ArchiveHistoryRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.DomainName == "" {
		return errEmptyDomainName
	}
	return nil
}

// ValidateGetRequest validates the get archived history request
func ValidateGetRequest(request *GetHistoryRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.PageSize == 0 {
		return errInvalidPageSize
	}
	return nil
}

// ValidateVisibilityArchivalRequest validates the archive visibility request
func ValidateVisibilityArchivalRequest(request *ArchiveVisibilityRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
	}
	if request.DomainName == "" {
		return errEmptyDomainName
	}
	if request.WorkflowID == "" {
		return errEmptyWorkflowID
	}
	if request.RunID == "" {
		return errEmptyRunID
	}
	if request.WorkflowTypeName == "" {
		return errEmptyWorkflowTypeName
	}
	if request.StartTimestamp == 0 {
		return errEmptyStartTime
	}
	if request.CloseTimestamp == 0 {
		return errEmptyCloseTime
	}
	return nil
}

// ValidateQueryRequest validates the query visibility request
func ValidateQueryRequest(request *QueryVisibilityRequest) error {
	if request.DomainID == "" {
		return errEmptyDomainID
	}
	if request.PageSize == 0 {
		return errInvalidPageSize
	}
	if request.Query == "" {
		return errEmptyQuery
	}
	return nil
}

// ConvertSearchAttrToBytes converts search attribute value from string back to byte array
func ConvertSearchAttrToBytes(searchAttrStr map[string]string) map[string][]byte {
	searchAttr := make(map[string][]byte)
	for k, v := range searchAttrStr {
		searchAttr[k] = []byte(v)
	}
	return searchAttr
}

func IsHistoryMutated(request *ArchiveHistoryRequest, historyBatches []*types.History, isLast bool, logger log.Logger) (mutated bool) {
	lastBatch := historyBatches[len(historyBatches)-1].Events
	lastEvent := lastBatch[len(lastBatch)-1]
	lastFailoverVersion := lastEvent.Version
	defer func() {
		if mutated {
			logger.Warn(ArchiveNonRetriableErrorMsg+":history is mutated when during archival",
				tag.ArchivalArchiveFailReason(ErrReasonHistoryMutated),
				tag.FailoverVersion(lastFailoverVersion),
				tag.TokenLastEventID(lastEvent.ID))
		}
	}()
	if lastFailoverVersion > request.CloseFailoverVersion {
		return true
	}

	if !isLast {
		return false
	}
	lastEventID := lastEvent.ID
	return lastFailoverVersion != request.CloseFailoverVersion || lastEventID+1 != request.NextEventID
}
