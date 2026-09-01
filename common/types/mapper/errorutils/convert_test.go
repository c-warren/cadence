package errorutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertError(t *testing.T) {
	t.Run("sample error", func(t *testing.T) {
		err := &sampleError{
			message: "test",
		}
		isError, converted := ConvertError(err, sampleErrorConvertor)
		assert.True(t, isError, "is error")
		assert.Error(t, converted, "converted error")
		assert.Equal(t, err.message, converted.message)
	})
	t.Run("nil error is propagated as nil", func(t *testing.T) {
		isError, converted := ConvertError(nil, sampleErrorConvertor)
		assert.False(t, isError, "is error")
		assert.Nil(t, converted, "converted error")
	})
}

type sampleError struct {
	message string
}

func (s *sampleError) Error() string {
	return "sample error"
}

type convertedError struct {
	message string
}

func (c *convertedError) Error() string {
	return "converted error"
}

func sampleErrorConvertor(e *sampleError) *convertedError {
	return &convertedError{
		message: e.message,
	}
}
