package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPeerHostnameError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		peer     string
		wantErr  bool
		wantPeer string
	}{
		{
			name:     "nil error returns nil",
			err:      nil,
			peer:     "host1",
			wantErr:  false,
			wantPeer: "",
		},
		{
			name:     "empty peer returns original error",
			err:      errors.New("original error"),
			peer:     "",
			wantErr:  true,
			wantPeer: "",
		},
		{
			name:     "wraps error with peer",
			err:      errors.New("original error"),
			peer:     "host1",
			wantErr:  true,
			wantPeer: "host1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewPeerHostnameError(tt.err, tt.peer)
			if !tt.wantErr {
				assert.Nil(t, got)
				return
			}

			assert.NotNil(t, got)
			if tt.wantPeer == "" {
				assert.Equal(t, tt.err, got)
			} else {
				var peerErr *PeerHostnameError
				assert.True(t, errors.As(got, &peerErr))
				assert.Equal(t, tt.wantPeer, peerErr.PeerHostname)
				assert.Equal(t, tt.err, peerErr.WrappedError)
			}
		})
	}
}

func TestExtractPeerHostname(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantPeer string
		wantErr  error
	}{
		{
			name:     "nil error",
			err:      nil,
			wantPeer: "",
			wantErr:  nil,
		},
		{
			name:     "non-peer error",
			err:      errors.New("some error"),
			wantPeer: "",
			wantErr:  errors.New("some error"),
		},
		{
			name:     "peer error",
			err:      NewPeerHostnameError(errors.New("original error"), "host1"),
			wantPeer: "host1",
			wantErr:  errors.New("original error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer, err := ExtractPeerHostname(tt.err)
			assert.Equal(t, tt.wantPeer, peer)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}
