package testing

import (
	"reflect"
	"testing"

	"github.com/uber/cadence/common/types/testdata"
)

func TestAllFieldsSetInTestErrors(t *testing.T) {
	for _, err := range testdata.Errors {
		name := reflect.TypeOf(err).Elem().Name()
		t.Run(name, func(t *testing.T) {
			// Test all fields are set in the error
			allIsSet(t, err)
		})
	}
}
