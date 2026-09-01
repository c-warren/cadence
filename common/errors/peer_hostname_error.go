package errors

import (
	"errors"
	"fmt"
)

// PeerHostnameError wraps an error with peer hostname information
type PeerHostnameError struct {
	PeerHostname string
	WrappedError error
}

// Error implements the error interface
func (e *PeerHostnameError) Error() string {
	return fmt.Sprintf("peer hostname: %s, error: %v", e.PeerHostname, e.WrappedError)
}

// Unwrap implements the error unwrapping interface
func (e *PeerHostnameError) Unwrap() error {
	return e.WrappedError
}

// NewPeerHostnameError creates a new PeerHostnameError
func NewPeerHostnameError(err error, peer string) error {
	if err == nil {
		return nil
	}
	if peer == "" {
		return err
	}
	return &PeerHostnameError{
		PeerHostname: peer,
		WrappedError: err,
	}
}

// ExtractPeerHostname extracts the peer hostname from a wrapped error
// Returns the hostname and the original unwrapped error
func ExtractPeerHostname(err error) (string, error) {
	if err == nil {
		return "", nil
	}
	var peerErr *PeerHostnameError
	current := err
	if errors.As(current, &peerErr) {
		return peerErr.PeerHostname, peerErr.WrappedError
	}
	return "", err
}
