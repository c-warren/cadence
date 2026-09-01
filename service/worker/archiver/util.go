package archiver

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/dgryski/go-farm"
	"go.uber.org/cadence/activity"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

// MaxArchivalIterationTimeout returns the max allowed timeout for a single iteration of archival workflow
func MaxArchivalIterationTimeout() time.Duration {
	return workflowStartToCloseTimeout / 2
}

func hash(i interface{}) uint64 {
	var b bytes.Buffer
	// please make sure encoder is deterministic (especially when encoding map objects)
	// use json not gob here as json will sort map keys, while gob is non-deterministic
	json.NewEncoder(&b).Encode(i) //nolint:errcheck
	return farm.Fingerprint64(b.Bytes())
}

func hashesEqual(a []uint64, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	aMap := make(map[uint64]int)
	for _, elem := range a {
		aMap[elem] = aMap[elem] + 1
	}
	for _, elem := range b {
		count := aMap[elem]
		if count == 0 {
			return false
		}
		aMap[elem] = aMap[elem] - 1
	}
	return true
}

func tagLoggerWithHistoryRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return logger.WithTags(
		tag.ShardID(request.ShardID),
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalRequestBranchToken(request.BranchToken),
		tag.ArchivalRequestNextEventID(request.NextEventID),
		tag.ArchivalRequestCloseFailoverVersion(request.CloseFailoverVersion),
		tag.ArchivalURI(request.URI),
	)
}

func tagLoggerWithVisibilityRequest(logger log.Logger, request *ArchiveRequest) log.Logger {
	return logger.WithTags(
		tag.ArchivalRequestDomainID(request.DomainID),
		tag.ArchivalRequestDomainName(request.DomainName),
		tag.ArchivalRequestWorkflowID(request.WorkflowID),
		tag.ArchivalRequestRunID(request.RunID),
		tag.ArchivalURI(request.URI),
	)
}

func tagLoggerWithActivityInfo(logger log.Logger, activityInfo activity.Info) log.Logger {
	return logger.WithTags(
		tag.WorkflowID(activityInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(activityInfo.WorkflowExecution.RunID),
		tag.Attempt(activityInfo.Attempt))
}

func convertSearchAttributesToString(searchAttr map[string][]byte) map[string]string {
	searchAttrStr := make(map[string]string)
	for k, v := range searchAttr {
		searchAttrStr[k] = string(v)
	}
	return searchAttrStr
}
