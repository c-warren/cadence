package pinot

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/uber/cadence/common/types"
)

type (
	// PinotVisibilityPageToken holds the paging token for Pinot
	PinotVisibilityPageToken struct {
		From int
	}
)

// DeserializePageToken return the structural token
func DeserializePageToken(data []byte) (*PinotVisibilityPageToken, error) {
	var token PinotVisibilityPageToken
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
func SerializePageToken(token *PinotVisibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("unable to serialize page token. err: %v", err),
		}
	}
	return data, nil
}

// GetNextPageToken returns the structural token with nil handling
func GetNextPageToken(token []byte) (*PinotVisibilityPageToken, error) {
	var result *PinotVisibilityPageToken
	var err error
	if len(token) > 0 {
		result, err = DeserializePageToken(token)
		if err != nil {
			return nil, err
		}
	} else {
		result = &PinotVisibilityPageToken{}
	}
	return result, nil
}
