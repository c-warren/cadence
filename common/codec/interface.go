//go:generate mockgen -package $GOPACKAGE -destination interfaces_mock.go -self_package github.com/uber/cadence/common/codec github.com/uber/cadence/common/codec BinaryEncoder

package codec

import (
	"go.uber.org/thriftrw/protocol/stream"
	"go.uber.org/thriftrw/wire"

	"github.com/uber/cadence/common/types"
)

type (
	// BinaryEncoder represent the encoder which can serialize or deserialize object
	BinaryEncoder interface {
		Encode(obj ThriftObject) ([]byte, error)
		Decode(payload []byte, val ThriftObject) error
	}

	// ThriftObject represents a thrift object
	ThriftObject interface {
		FromWire(w wire.Value) error
		ToWire() (wire.Value, error)
		Encode(stream.Writer) error
		Decode(stream.Reader) error
	}
)

const (
	// used by thriftrw binary codec
	preambleVersion0 byte = 0x59
)

var (
	// MissingBinaryEncodingVersion indicate that the encoding version is missing
	MissingBinaryEncodingVersion = &types.BadRequestError{Message: "Missing binary encoding version."}
	// InvalidBinaryEncodingVersion indicate that the encoding version is incorrect
	InvalidBinaryEncodingVersion = &types.BadRequestError{Message: "Invalid binary encoding version."}
	// MsgPayloadNotThriftEncoded indicate message is not thrift encoded
	MsgPayloadNotThriftEncoded = &types.BadRequestError{Message: "Message payload is not thrift encoded."}
)
