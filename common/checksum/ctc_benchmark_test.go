package checksum

import (
	"testing"
	"time"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

func BenchmarkGenerateCRC32(b *testing.B) {
	obj := &shared.WorkflowExecutionInfo{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(uuid.New()),
			RunId:      common.StringPtr(uuid.New()),
		},
		StartTime:     common.Int64Ptr(time.Now().UnixNano()),
		HistoryLength: common.Int64Ptr(550),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checksum, _ := GenerateCRC32(obj, 1)
		_ = Verify(obj, checksum)
	}
}
