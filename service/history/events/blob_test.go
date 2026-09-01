package events

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/persistence"
)

func TestPersistedBlobs_Find(t *testing.T) {
	blob1 := persistence.DataBlob{Data: []byte{1, 2, 3}}
	blob2 := persistence.DataBlob{Data: []byte{4, 5, 6}}
	blob3 := persistence.DataBlob{Data: []byte{7, 8, 9}}
	branchA := []byte{11, 11, 11}
	branchB := []byte{22, 22, 22}
	persistedBlobs := PersistedBlobs{
		PersistedBlob{BranchToken: branchA, FirstEventID: 100, DataBlob: blob1},
		PersistedBlob{BranchToken: branchA, FirstEventID: 105, DataBlob: blob2},
		PersistedBlob{BranchToken: branchB, FirstEventID: 100, DataBlob: blob3},
	}
	assert.Equal(t, blob1, *persistedBlobs.Find(branchA, 100))
	assert.Equal(t, blob2, *persistedBlobs.Find(branchA, 105))
	assert.Equal(t, blob3, *persistedBlobs.Find(branchB, 100))
	assert.Nil(t, persistedBlobs.Find(branchB, 105))
	assert.Nil(t, persistedBlobs.Find([]byte{99}, 100))
}
