package rpc

// RPCError wraps errors to mark them as coming from RPC, i.e. basically expected.
type RPCError struct {
	cause error
}

// SerializationError wraps errors to mark them as coming from (de)serialization.
//
// In practice this should not ever occur, as it should imply one of:
// - incorrect Any Value/ValueType pairing, which should not pass tests
// - non-backwards-compatible type changes deployed in an unsafe way
// - incompatible Thrift-binary changes
type SerializationError struct {
	cause error
}
type errwrapper interface {
	error
	Unwrap() error
}

var _ errwrapper = (*RPCError)(nil)
var _ errwrapper = (*SerializationError)(nil)

func (e *RPCError) Error() string           { return e.cause.Error() }
func (e *RPCError) Unwrap() error           { return e.cause }
func (e *SerializationError) Error() string { return e.cause.Error() }
func (e *SerializationError) Unwrap() error { return e.cause }
