package clock

import (
	"testing"
	"time"
)

func BenchmarkRealTimerGate(b *testing.B) {
	timer := NewTimerGate(NewRealTimeSource())

	for i := 0; i < b.N; i++ {
		timer.Update(time.Now())
	}
}
