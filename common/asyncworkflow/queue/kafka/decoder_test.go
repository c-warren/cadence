package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/types"
)

func TestDecode(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}

	tests := []struct {
		name           string
		blob           *types.DataBlob
		want           *testStruct
		wantErr        bool
		expectedErrMsg string
	}{
		{
			name: "valid JSON encoding",
			blob: &types.DataBlob{
				Data:         []byte(`{"name":"test"}`),
				EncodingType: types.EncodingTypeJSON.Ptr(),
			},
			want:    &testStruct{Name: "test"},
			wantErr: false,
		},
		{
			name: "unsupported encoding type",
			blob: &types.DataBlob{
				Data:         []byte("aa"),
				EncodingType: types.EncodingTypeThriftRW.Ptr(),
			},
			want:           nil,
			wantErr:        true,
			expectedErrMsg: "unsupported encoding type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder := newDecoder(tt.blob)
			var got testStruct
			err := decoder.Decode(&got)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErrMsg)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, &got)
			}
		})
	}
}
