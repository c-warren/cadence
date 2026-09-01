package sql

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	RegisterPlugin("shared", &fakePlugin{})
	os.Exit(m.Run())
}
