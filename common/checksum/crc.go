package checksum

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/uber/cadence/common/codec"
)

// GenerateCRC32 generates an IEEE crc32 checksum on the
// serilized byte array of the given thrift object. The
// serialization proto used will be of type thriftRW
func GenerateCRC32(
	payload codec.ThriftObject,
	payloadVersion int,
) (Checksum, error) {

	encoder := codec.NewThriftRWEncoder()
	payloadBytes, err := encoder.Encode(payload)
	if err != nil {
		return Checksum{}, err
	}

	crc := crc32.ChecksumIEEE(payloadBytes)
	checksum := make([]byte, 4)
	binary.BigEndian.PutUint32(checksum, crc)
	return Checksum{
		Value:   checksum,
		Version: payloadVersion,
		Flavor:  FlavorIEEECRC32OverThriftBinary,
	}, nil
}

// Verify verifies that the checksum generated from the
// given thrift object matches the specified expected checksum
// Return ErrMismatch when checksums mismatch
func Verify(
	payload codec.ThriftObject,
	checksum Checksum,
) error {

	if !checksum.Flavor.IsValid() || checksum.Flavor != FlavorIEEECRC32OverThriftBinary {
		return fmt.Errorf("unknown checksum flavor %v", checksum.Flavor)
	}

	expected, err := GenerateCRC32(payload, checksum.Version)
	if err != nil {
		return err
	}

	if !bytes.Equal(expected.Value, checksum.Value) {
		return ErrMismatch
	}

	return nil
}
