package events

import (
	"bytes"

	"github.com/uber/cadence/common/persistence"
)

type (
	// PersistedBlob is a wrapper on persistence.DataBlob with additional field indicating what was persisted.
	// Additional fields are used as an identification key among other blobs.
	PersistedBlob struct {
		persistence.DataBlob

		BranchToken  []byte
		FirstEventID int64
	}
	// PersistedBlobs is a slice of PersistedBlob
	PersistedBlobs []PersistedBlob
)

// Find searches for persisted event blob. Returns nil when not found.
func (blobs PersistedBlobs) Find(branchToken []byte, firstEventID int64) *persistence.DataBlob {
	// Linear search is ok here, as we will only have 1-2 persisted blobs per transaction
	for _, blob := range blobs {
		if bytes.Equal(blob.BranchToken, branchToken) && blob.FirstEventID == firstEventID {
			return &blob.DataBlob
		}
	}
	return nil
}
