//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination generic_client_mock.go -self_package github.com/uber/cadence/common/pinot

package pinot

import p "github.com/uber/cadence/common/persistence"

type (
	// GenericClient is a generic interface for all versions of Pinot clients
	GenericClient interface {
		// Search API is only for supporting various List[Open/Closed]WorkflowExecutions(ByXyz).
		// Use SearchByQuery or ScanByQuery for generic purpose searching.
		Search(request *SearchRequest) (*SearchResponse, error)
		SearchAggr(request *SearchRequest) (AggrResponse, error)
		// CountByQuery is for returning the count of workflow executions that match the query
		CountByQuery(query string) (int64, error)
		GetTableName() string
	}

	// IsRecordValidFilter is a function to filter visibility records
	IsRecordValidFilter func(rec *p.InternalVisibilityWorkflowExecutionInfo) bool

	// SearchRequest is request for Search
	SearchRequest struct {
		Query           string
		IsOpen          bool
		Filter          IsRecordValidFilter
		MaxResultWindow int
		ListRequest     *p.InternalListWorkflowExecutionsRequest
	}

	// SearchResponse is a response to Search, SearchByQuery and ScanByQuery
	SearchResponse = p.InternalListWorkflowExecutionsResponse
	AggrResponse   [][]interface{}
)
