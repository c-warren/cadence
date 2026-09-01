//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/common/asyncworkflow/queue

package queue

import (
	"github.com/uber/cadence/common/asyncworkflow/queue/provider"
	"github.com/uber/cadence/common/types"
)

type (
	// Provider is used to get a queue
	Provider interface {
		GetPredefinedQueue(string) (provider.Queue, error)
		GetQueue(string, *types.DataBlob) (provider.Queue, error)
	}
)
