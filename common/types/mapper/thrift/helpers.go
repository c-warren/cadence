package thrift

import (
	"time"

	"github.com/uber/cadence/common"
)

func timeToNano(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	return common.Int64Ptr(t.UnixNano())
}

func nanoToTime(nanos *int64) *time.Time {
	if nanos == nil {
		return nil
	}

	result := time.Unix(0, *nanos)
	return &result
}

func timeValToNano(t time.Time) *int64 {
	if t.IsZero() {
		return nil
	}
	return common.Int64Ptr(t.UnixNano())
}

func nanoToTimeVal(n *int64) time.Time {
	if n == nil {
		return time.Time{}
	}
	return time.Unix(0, *n)
}

func durationToSeconds(d time.Duration) *int32 {
	if d == 0 {
		return nil
	}
	s := int32(d / time.Second)
	return &s
}

func secondsToDuration(s *int32) time.Duration {
	if s == nil {
		return 0
	}
	return time.Duration(*s) * time.Second
}
