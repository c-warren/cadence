package checksum

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

func TestCRC32OverThrift(t *testing.T) {
	// note: do not use a struct with map since
	// iteration order is not guaranteed in Go and
	// so, each call to thrift encode will result in
	// different set of serialized bytes
	obj := &shared.WorkflowExecutionInfo{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(uuid.New()),
			RunId:      common.StringPtr(uuid.New()),
		},
		StartTime:     common.Int64Ptr(time.Now().UnixNano()),
		HistoryLength: common.Int64Ptr(550),
	}

	parallism := 10
	loopCount := 100
	successCount := int64(0)

	startC := make(chan struct{})
	doneWG := sync.WaitGroup{}
	doneWG.Add(parallism)

	for i := 0; i < parallism; i++ {
		go func() {
			defer doneWG.Done()
			<-startC
			for count := 0; count < loopCount; count++ {
				csum, err := GenerateCRC32(obj, 1)
				if err != nil {
					return
				}
				if err := Verify(obj, csum); err != nil {
					return
				}
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}

	close(startC)
	success := common.AwaitWaitGroup(&doneWG, time.Second)
	assert.True(t, success, "timed out waiting for goroutines to finish")
	assert.Equal(t, int64(parallism*loopCount), successCount)
}
