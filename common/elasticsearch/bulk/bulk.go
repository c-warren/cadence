package bulk

import (
	"context"
	"time"
)

const UnknownStatusCode = -1

type GenericBulkableRequestType int

const (
	BulkableIndexRequest GenericBulkableRequestType = iota
	BulkableDeleteRequest
	BulkableCreateRequest
)

type (
	// GenericBulkProcessor is a bulk processor
	GenericBulkProcessor interface {
		Start(ctx context.Context) error
		Stop() error
		Close() error
		Add(request *GenericBulkableAddRequest)
		Flush() error
	}

	// BulkProcessorParameters holds all required and optional parameters for executing bulk service
	BulkProcessorParameters struct {
		Name          string
		NumOfWorkers  int
		BulkActions   int
		BulkSize      int
		FlushInterval time.Duration
		Backoff       GenericBackoff
		BeforeFunc    GenericBulkBeforeFunc
		AfterFunc     GenericBulkAfterFunc
	}

	// GenericBackoff allows callers to implement their own Backoff strategy.
	GenericBackoff interface {
		// Next implements a BackoffFunc.
		Next(retry int) (time.Duration, bool)
	}

	// GenericBulkBeforeFunc defines the signature of callbacks that are executed
	// before a commit to Elasticsearch.
	GenericBulkBeforeFunc func(executionId int64, requests []GenericBulkableRequest)

	// GenericBulkAfterFunc defines the signature of callbacks that are executed
	// after a commit to Elasticsearch. The err parameter signals an error.
	GenericBulkAfterFunc func(executionId int64, requests []GenericBulkableRequest, response *GenericBulkResponse, err *GenericError)

	// GenericBulkableRequest is a generic interface to bulkable requests.
	GenericBulkableRequest interface {
		String() string
		Source() ([]string, error)
	}

	// GenericBulkableAddRequest a struct to hold a bulk request
	GenericBulkableAddRequest struct {
		Index       string
		Type        string
		ID          string
		VersionType string
		Version     int64
		// request types can be index, delete or create
		RequestType GenericBulkableRequestType
		// should be nil if IsDelete is true
		Doc interface{}
	}

	// GenericBulkResponse is generic struct of bulk response
	GenericBulkResponse struct {
		Took   int                                   `json:"took,omitempty"`
		Errors bool                                  `json:"errors,omitempty"`
		Items  []map[string]*GenericBulkResponseItem `json:"items,omitempty"`
	}

	// GenericError encapsulates error status and details returned from Elasticsearch.
	GenericError struct {
		Status  int   `json:"status"`
		Details error `json:"error,omitempty"`
	}

	// GenericBulkResponseItem is the result of a single bulk request.
	GenericBulkResponseItem struct {
		Index   string `json:"_index,omitempty"`
		Type    string `json:"_type,omitempty"`
		ID      string `json:"_id,omitempty"`
		Version int64  `json:"_version,omitempty"`
		Result  string `json:"result,omitempty"`
		Status  int    `json:"status,omitempty"`
		// the error details
		Error interface{}
	}
)
