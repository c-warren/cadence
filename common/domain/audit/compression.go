package audit

import (
	"encoding/json"

	"github.com/golang/snappy"

	"github.com/uber/cadence/common/persistence"
)

const (
	// EncodingJSONSnappy represents JSON serialization with Snappy compression
	EncodingJSONSnappy = "json-snappy"
)

// SerializeAndCompress takes a domain state and returns compressed JSON bytes
func SerializeAndCompress(domain *persistence.GetDomainResponse) ([]byte, error) {
	// Marshal to JSON
	jsonBytes, err := json.Marshal(domain)
	if err != nil {
		return nil, err
	}

	// Compress with Snappy
	compressed := snappy.Encode(nil, jsonBytes)
	return compressed, nil
}

// DecompressAndDeserialize reverses the SerializeAndCompress operation
func DecompressAndDeserialize(compressed []byte) (*persistence.GetDomainResponse, error) {
	// Decompress
	jsonBytes, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}

	// Unmarshal
	var domain persistence.GetDomainResponse
	if err := json.Unmarshal(jsonBytes, &domain); err != nil {
		return nil, err
	}

	return &domain, nil
}
