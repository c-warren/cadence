package thrift

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/uber/cadence/common/types/testdata"
)

func TestErrors(t *testing.T) {
	for _, err := range testdata.Errors {
		name := reflect.TypeOf(err).Elem().Name()
		t.Run(name, func(t *testing.T) {
			// Test that the mappings does not lose information
			assert.Equal(t, err, ToError(FromError(err)))
		})
	}
}

func TestNilMapsToNil(t *testing.T) {
	assert.Nil(t, FromError(nil))
	assert.Nil(t, ToError(nil))
}

func TestFromUnknownErrorMapsToItself(t *testing.T) {
	err := errors.New("unknown error")
	assert.Equal(t, err, FromError(err))
}

func TestToUnknownErrorMapsToItself(t *testing.T) {
	err := yarpcerrors.DeadlineExceededErrorf("timeout")
	assert.Equal(t, err, ToError(err))
}
