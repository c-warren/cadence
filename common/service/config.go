package service

import (
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

type (
	// Config is a subset of the service dynamic config for single service
	Config struct {
		PersistenceMaxQPS       dynamicproperties.IntPropertyFn
		PersistenceGlobalMaxQPS dynamicproperties.IntPropertyFn
		ThrottledLoggerMaxRPS   dynamicproperties.IntPropertyFn

		// WriteVisibilityStoreName is the write mode of visibility
		WriteVisibilityStoreName dynamicproperties.StringPropertyFn
		// EnableLogCustomerQueryParameter is to enable log customer parameters
		EnableLogCustomerQueryParameter dynamicproperties.BoolPropertyFnWithDomainFilter
		// ReadVisibilityStoreName is the read store for visibility
		ReadVisibilityStoreName dynamicproperties.StringPropertyFnWithDomainFilter

		// configs for db visibility
		EnableDBVisibilitySampling                  dynamicproperties.BoolPropertyFn                `yaml:"-" json:"-"`
		EnableReadDBVisibilityFromClosedExecutionV2 dynamicproperties.BoolPropertyFn                `yaml:"-" json:"-"`
		WriteDBVisibilityOpenMaxQPS                 dynamicproperties.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`
		WriteDBVisibilityClosedMaxQPS               dynamicproperties.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`
		DBVisibilityListMaxQPS                      dynamicproperties.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`

		// configs for es visibility
		ESIndexMaxResultWindow          dynamicproperties.IntPropertyFn `yaml:"-" json:"-"`
		ValidSearchAttributes           dynamicproperties.MapPropertyFn `yaml:"-" json:"-"`
		PinotOptimizedQueryColumns      dynamicproperties.MapPropertyFn `yaml:"-" json:"-"`
		SearchAttributesHiddenValueKeys dynamicproperties.MapPropertyFn `yaml:"-" json:"-"`
		// deprecated: never read from, all ES reads and writes erroneously use PersistenceMaxQPS
		ESVisibilityListMaxQPS dynamicproperties.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`

		IsErrorRetryableFunction backoff.IsRetryable
	}
)
