package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/filestore"
	"github.com/uber/cadence/common/config"
)

func TestBlobstoreWriter(t *testing.T) {
	type testCase struct {
		name        string
		input       string
		input2      string
		expectedErr bool
	}

	testCases := []testCase{
		{
			name:        "Normal case",
			input:       "one",
			input2:      "two",
			expectedErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uuid := "test-uuid"
			extension := Extension("test")
			outputDir := t.TempDir()

			cfg := &config.FileBlobstore{
				OutputDirectory: outputDir,
			}
			// Reusing the FilestoreClient from the other sister test in the same package.
			blobstoreClient, err := filestore.NewFilestoreClient(cfg)
			require.NoError(t, err)

			blobstoreWriter := NewBlobstoreWriter(uuid, extension, blobstoreClient, 10).(*blobstoreWriter)
			// Add data to the writer
			err = blobstoreWriter.Add(tc.input)
			err = blobstoreWriter.Add(tc.input2)
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Flush the writer to write data to the blobstore
			err = blobstoreWriter.Flush()
			assert.NoError(t, err)

			// Retrieve the keys of flushed data
			flushedKeys := blobstoreWriter.FlushedKeys()
			if flushedKeys == nil {
				t.Fatal("Expected flushedKeys to be not nil")
			}

			// Read back the data from the blobstore
			key := pageNumberToKey(uuid, extension, flushedKeys.MinPage)
			req := &blobstore.GetRequest{Key: key}
			ctx := context.Background()
			resp, err := blobstoreClient.Get(ctx, req)
			assert.NoError(t, err)

			// Verify the contents
			assert.Equal(t, string(resp.Blob.Body), "\"one\"\r\n\"two\"\r\n")
		})
	}
}
