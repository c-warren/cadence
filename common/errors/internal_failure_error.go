package errors

type (
	// InternalFailureError represents unexpected case happening or a code bug
	InternalFailureError struct {
		Msg string
	}
)

// NewInternalFailureError return internal failure error
func NewInternalFailureError(msg string) *InternalFailureError {
	return &InternalFailureError{Msg: msg}
}

func (e *InternalFailureError) Error() string {
	return e.Msg
}
