package defaultisolationgroupstate

import (
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/types"
)

// IsolationGroups is an internal convenience return type of a collection of IsolationGroup configurations
type isolationGroups struct {
	Global types.IsolationGroupConfiguration
	Domain types.IsolationGroupConfiguration
}

// defaultConfig values for the partitioning library for segmenting portions of workflows into isolation-groups - a resiliency
// concept meant to help move workflows around and away from failure zones.
type defaultConfig struct {
	// IsolationGroupEnabled is a domain-based configuration value for whether this feature is enabled at all
	IsolationGroupEnabled dynamicproperties.BoolPropertyFnWithDomainFilter
	// AllIsolationGroups is all the possible isolation-groups available for a region
	AllIsolationGroups func() []string
}
