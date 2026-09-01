package testing

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func allIsSet(t *testing.T, err error) {
	// All the errors are pointers, so we get the value with .Elem
	errValue := reflect.ValueOf(err).Elem()

	for i := 0; i < errValue.NumField(); i++ {
		field := errValue.Field(i)

		// IsZero checks if the value is the default value (e.g. nil, "", 0 etc)
		assert.True(t, !field.IsZero(), "Field %s is not set for error %s", errValue.Type().Field(i).Name, errValue.Type())
	}
}
