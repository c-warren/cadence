package engineimpl

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/uber/cadence/service/history/engine/testdata"
)

func TestHistoryEngineStartStop(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	eft := testdata.NewEngineForTest(t, NewEngineWithShardContext)

	eft.Engine.Start()
	eft.Engine.Stop()
}
