package api

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShuttingDownError(t *testing.T) {
	wh, _ := setupMocksForWorkflowHandler(t)
	wh.Stop()

	// get all methods of Handler interface
	tt := reflect.TypeOf(struct{ Handler }{})
	methodNames := make(map[string]struct{})
	for i := 0; i < tt.NumMethod(); i++ {
		methodNames[tt.Method(i).Name] = struct{}{}
	}
	delete(methodNames, "GetClusterInfo")
	delete(methodNames, "Health")

	v := reflect.ValueOf(wh)
	for name := range methodNames {
		method := v.MethodByName(name)
		methodType := method.Type()
		if methodType.Kind() != reflect.Func {
			t.Fatalf("method: %s is not a function - %s", name, methodType.String())
		}
		if methodType.IsVariadic() {
			t.Fatalf("method: %s is variadic - %s", name, methodType.String())
		}
		if methodType.NumIn() < 1 || methodType.NumIn() > 2 {
			t.Fatalf("method: %s has wrong number of inputs - %s", name, methodType.String())
		}

		var results []reflect.Value
		if methodType.NumIn() == 1 {
			results = method.Call([]reflect.Value{reflect.ValueOf(context.Background())})
		} else {
			results = method.Call([]reflect.Value{reflect.ValueOf(context.Background()), reflect.Zero(methodType.In(1))})
		}
		if len(results) == 1 {
			err, ok := results[0].Interface().(error)
			if !ok {
				t.Fatalf("method: %s has wrong output type - %s", name, methodType.String())
			}
			assert.ErrorContains(t, err, "Shutting down")
		} else if len(results) == 2 {
			err, ok := results[1].Interface().(error)
			if !ok {
				t.Fatalf("method: %s has wrong output type - %s", name, methodType.String())
			}
			assert.ErrorContains(t, err, "Shutting down")
		} else {
			t.Fatalf("method: %s has wrong number of outputs - %s", name, methodType.String())
		}
	}
}
