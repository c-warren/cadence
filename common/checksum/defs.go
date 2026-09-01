package checksum

import "errors"

type (
	// Checksum represents a checksum value along
	// with associated metadata
	Checksum struct {
		// Version represents version of the payload from
		Version int
		// which this checksum was derived
		Flavor Flavor
		// Value is the checksum value
		Value []byte
	}

	// Flavor is an enum type that represents the type of checksum
	Flavor int
)

const (
	// FlavorUnknown represents an unknown/uninitialized checksum flavor
	FlavorUnknown Flavor = iota
	// FlavorIEEECRC32OverThriftBinary represents crc32 checksum generated over thriftRW serialized payload
	FlavorIEEECRC32OverThriftBinary
	maxFlavors
)

// ErrMismatch indicates a checksum verification failure due to
// a derived checksum not being equal to expected checksum
var ErrMismatch = errors.New("checksum mismatch error")

// IsValid returns true if the checksum flavor is valid
func (f Flavor) IsValid() bool {
	return f > FlavorUnknown && f < maxFlavors
}
