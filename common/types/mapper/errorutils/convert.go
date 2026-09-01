package errorutils

import "errors"

// ConvertError checks if an error is of type T and if so, converts it using f.
func ConvertError[T, V error](err error, fn func(T) V) (bool, V) {
	var (
		e   T
		res V
	)
	if !errors.As(err, &e) {
		return false, res
	}
	return true, fn(e)
}
