package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/uber/cadence/common/types"
)

type (
	// ElasticVisibilityPageToken holds the paging token for ElasticSearch
	ElasticVisibilityPageToken struct {
		// for ES API From+Size
		From int
		// for ES API searchAfter
		SortValue  interface{}
		TieBreaker string // runID
		// for ES scroll API
		ScrollID string
	}
)

// DeserializePageToken return the structural token
func DeserializePageToken(data []byte) (*ElasticVisibilityPageToken, error) {
	var token ElasticVisibilityPageToken
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err := dec.Decode(&token)
	if err != nil {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("unable to deserialize page token. err: %v", err),
		}
	}
	return &token, nil
}

// SerializePageToken return the token blob
func SerializePageToken(token *ElasticVisibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("unable to serialize page token. err: %v", err),
		}
	}
	return data, nil
}

// GetNextPageToken returns the structural token with nil handling
func GetNextPageToken(token []byte) (*ElasticVisibilityPageToken, error) {
	var result *ElasticVisibilityPageToken
	var err error
	if len(token) > 0 {
		result, err = DeserializePageToken(token)
		if err != nil {
			return nil, err
		}
	} else {
		result = &ElasticVisibilityPageToken{}
	}
	return result, nil
}

// ShouldSearchAfter decides if should search after
func ShouldSearchAfter(token *ElasticVisibilityPageToken) bool {
	return token.TieBreaker != ""
}
