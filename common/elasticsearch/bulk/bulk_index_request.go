package bulk

import (
	"encoding/json"
	"fmt"
	"strings"
)

var _ GenericBulkableRequest = (*BulkIndexRequest)(nil)

type BulkIndexRequest struct {
	index       string
	typ         string
	id          string
	opType      string
	version     *int64
	versionType string // default is "internal"
	doc         interface{}

	source []string
}

type bulkIndexRequestCommand map[string]bulkIndexRequestCommandOp

type bulkIndexRequestCommandOp struct {
	Index       string `json:"_index,omitempty"`
	ID          string `json:"_id,omitempty"`
	Version     *int64 `json:"version,omitempty"`
	VersionType string `json:"version_type,omitempty"`
}

// NewBulkIndexRequest returns a new BulkIndexRequest.
// The operation type is "index" by default.
func NewBulkIndexRequest() *BulkIndexRequest {
	return &BulkIndexRequest{
		opType: "index",
	}
}

// Index specifies the Elasticsearch index to use for this index request.
// If unspecified, the index set on the BulkService will be used.
func (r *BulkIndexRequest) Index(index string) *BulkIndexRequest {
	r.index = index
	r.source = nil
	return r
}

// Type specifies the Elasticsearch type to use for this index request.
// If unspecified, the type set on the BulkService will be used.
func (r *BulkIndexRequest) Type(typ string) *BulkIndexRequest {
	r.typ = typ
	r.source = nil
	return r
}

// ID specifies the identifier of the document to index.
func (r *BulkIndexRequest) ID(id string) *BulkIndexRequest {
	r.id = id
	r.source = nil
	return r
}

// OpType specifies if this request should follow create-only or upsert
// behavior. This follows the OpType of the standard document index API.
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/docs-index_.html#operation-type
// for details.
func (r *BulkIndexRequest) OpType(opType string) *BulkIndexRequest {
	r.opType = opType
	r.source = nil
	return r
}

// Version indicates the version of the document as part of an optimistic
// concurrency model.
func (r *BulkIndexRequest) Version(version int64) *BulkIndexRequest {
	r.version = &version
	r.source = nil
	return r
}

// VersionType specifies how versions are created. It can be e.g. internal,
// external, external_gte, or force.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/docs-index_.html#index-versioning
// for details.
func (r *BulkIndexRequest) VersionType(versionType string) *BulkIndexRequest {
	r.versionType = versionType
	r.source = nil
	return r
}

// Doc specifies the document to index.
func (r *BulkIndexRequest) Doc(doc interface{}) *BulkIndexRequest {
	r.doc = doc
	r.source = nil
	return r
}

// String returns the on-wire representation of the index request,
// concatenated as a single string.
func (r *BulkIndexRequest) String() string {
	lines, err := r.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return strings.Join(lines, "\n")
}

// Source returns the on-wire representation of the index request,
// split into an action-and-meta-data line and an (optional) source line.
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/docs-bulk.html
// for details.
func (r *BulkIndexRequest) Source() ([]string, error) {
	// { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
	// { "field1" : "value1" }

	if r.source != nil {
		return r.source, nil
	}

	lines := make([]string, 2)

	indexCommand := bulkIndexRequestCommandOp{
		Index:       r.index,
		ID:          r.id,
		Version:     r.version,
		VersionType: r.versionType,
	}
	command := bulkIndexRequestCommand{
		r.opType: indexCommand,
	}

	var err error
	var body []byte
	body, err = json.Marshal(command)

	if err != nil {
		return nil, err
	}

	lines[0] = string(body)

	// "field1" ...
	if r.doc != nil {
		body, err := json.Marshal(r.doc)
		if err != nil {
			return nil, err
		}
		lines[1] = string(body)
	} else {
		lines[1] = "{}"
	}

	r.source = lines
	return lines, nil
}
