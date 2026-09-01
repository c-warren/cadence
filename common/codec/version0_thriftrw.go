package codec

import (
	"bytes"

	"go.uber.org/thriftrw/protocol/binary"
)

type (
	// ThriftRWEncoder is an implementation using thrift rw for binary encoding / decoding
	// NOTE: this encoder only works for thrift struct
	ThriftRWEncoder struct {
	}
)

var _ BinaryEncoder = (*ThriftRWEncoder)(nil)

// NewThriftRWEncoder generate a new ThriftRWEncoder
func NewThriftRWEncoder() *ThriftRWEncoder {
	return &ThriftRWEncoder{}
}

// Encode encode the object
func (t *ThriftRWEncoder) Encode(obj ThriftObject) ([]byte, error) {
	if obj == nil {
		return nil, MsgPayloadNotThriftEncoded
	}
	var writer bytes.Buffer
	// use the first byte to version the serialization
	err := writer.WriteByte(preambleVersion0)
	if err != nil {
		return nil, err
	}

	sw := binary.Default.Writer(&writer)
	defer sw.Close()
	if err := obj.Encode(sw); err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}

// Decode decode the object
func (t *ThriftRWEncoder) Decode(b []byte, val ThriftObject) error {
	if len(b) < 1 {
		return MissingBinaryEncodingVersion
	}

	version := b[0]
	if version != preambleVersion0 {
		return InvalidBinaryEncodingVersion
	}

	reader := bytes.NewReader(b[1:])
	sr := binary.Default.Reader(reader)
	return val.Decode(sr)
}
