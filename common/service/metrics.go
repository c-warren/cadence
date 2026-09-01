package service

import (
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

// GetMetricsServiceIdx returns the metrics name
func GetMetricsServiceIdx(serviceName string, logger log.Logger) metrics.ServiceIdx {
	switch serviceName {
	case Frontend:
		return metrics.Frontend
	case History:
		return metrics.History
	case Matching:
		return metrics.Matching
	case Worker:
		return metrics.Worker
	default:
		logger.Fatal("Unknown service name for metrics!")
	}

	// this should never happen!
	return metrics.NumServices
}
