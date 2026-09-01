package batcher

import (
	"fmt"

	"github.com/uber/cadence/common"
)

func validateParams(params BatchParams) error {
	if params.BatchType == "" ||
		params.Reason == "" ||
		params.DomainName == "" ||
		params.Query == "" {
		return fmt.Errorf("must provide required parameters: BatchType/Reason/DomainName/Query")
	}
	switch params.BatchType {
	case BatchTypeSignal:
		if params.SignalParams.SignalName == "" {
			return fmt.Errorf("must provide signal name")
		}
		return nil
	case BatchTypeReplicate:
		if params.ReplicateParams.SourceCluster == "" {
			return fmt.Errorf("must provide source cluster")
		}
		if params.ReplicateParams.TargetCluster == "" {
			return fmt.Errorf("must provide target cluster")
		}
		return nil
	case BatchTypeCancel:
		fallthrough
	case BatchTypeTerminate:
		fallthrough
	case BatchTypeRefresh:
		return nil
	default:
		return fmt.Errorf("not supported batch type: %v", params.BatchType)
	}
}

func setDefaultParams(params BatchParams) BatchParams {
	if params.RPS <= 0 {
		params.RPS = DefaultRPS
	}
	if params.Concurrency <= 0 {
		params.Concurrency = DefaultConcurrency
	}
	if params.PageSize <= 0 {
		params.PageSize = DefaultPageSize
	}
	if params.AttemptsOnRetryableError <= 0 {
		params.AttemptsOnRetryableError = DefaultAttemptsOnRetryableError
	}
	if params.ActivityHeartBeatTimeout <= 0 {
		params.ActivityHeartBeatTimeout = DefaultActivityHeartBeatTimeout
	}
	if len(params.NonRetryableErrors) > 0 {
		params._nonRetryableErrors = make(map[string]struct{}, len(params.NonRetryableErrors))
		for _, estr := range params.NonRetryableErrors {
			params._nonRetryableErrors[estr] = struct{}{}
		}
	}
	if params.TerminateParams.TerminateChildren == nil {
		params.TerminateParams.TerminateChildren = common.BoolPtr(true)
	}
	if params.MaxActivityRetries < 0 {
		params.MaxActivityRetries = DefaultMaxActivityRetries
	}
	return params
}
