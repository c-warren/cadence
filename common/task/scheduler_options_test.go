package task

import (
	"testing"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

func TestSchedulerOptionsString(t *testing.T) {
	tests := []struct {
		desc            string
		schedulerType   int
		queueSize       int
		workerCount     dynamicproperties.IntPropertyFn
		dispatcherCount int
		wantErr         bool
		want            string
	}{
		{
			desc:            "FIFO",
			schedulerType:   int(SchedulerTypeFIFO),
			queueSize:       1,
			workerCount:     dynamicproperties.GetIntPropertyFn(3),
			dispatcherCount: 1,
			want:            "{schedulerType:1, fifoSchedulerOptions:{QueueSize: 1, WorkerCount: 3, DispatcherCount: 1}, wrrSchedulerOptions:<nil>}",
		},
		{
			desc:            "WRR",
			schedulerType:   int(SchedulerTypeWRR),
			queueSize:       3,
			workerCount:     dynamicproperties.GetIntPropertyFn(4),
			dispatcherCount: 5,
			want:            "{schedulerType:2, fifoSchedulerOptions:<nil>, wrrSchedulerOptions:{QueueSize: 3, DispatcherCount: 5}}",
		},
		{
			desc:          "InvalidSchedulerType",
			schedulerType: 3,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			o, err := NewSchedulerOptions[int, PriorityTask](tc.schedulerType, tc.queueSize, tc.workerCount, tc.dispatcherCount, nil, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("Got error: %v, wantErr: %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if got := o.String(); got != tc.want {
				t.Errorf("Got: %v, want: %v", got, tc.want)
			}
		})
	}
}
