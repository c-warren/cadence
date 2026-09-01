//go:generate mockgen -package $GOPACKAGE -destination predicate_mock.go github.com/uber/cadence/service/history/queuev2 Predicate

package queuev2

import (
	"maps"

	"github.com/uber/cadence/common/persistence"
)

type (
	// Predicate defines a predicate that can be used to filter tasks
	Predicate interface {
		// IsEmpty returns true if no task satisfies the predicate
		IsEmpty() bool
		// Check returns true if the task satisfies the predicate
		Check(task persistence.Task) bool
		// Equals returns true if the predicate is the same as the other predicate
		Equals(other Predicate) bool
	}

	domainIDPredicate struct {
		domainIDs   map[string]struct{}
		isExclusive bool
	}

	universalPredicate struct{}

	emptyPredicate struct{}
)

func NewUniversalPredicate() Predicate {
	return &universalPredicate{}
}

func (p *universalPredicate) IsEmpty() bool {
	return false
}

func (p *universalPredicate) Check(task persistence.Task) bool {
	return true
}

func (p *universalPredicate) Equals(other Predicate) bool {
	_, ok := other.(*universalPredicate)
	return ok
}

func NewEmptyPredicate() Predicate {
	return &emptyPredicate{}
}

func (p *emptyPredicate) IsEmpty() bool {
	return true
}

func (p *emptyPredicate) Check(task persistence.Task) bool {
	return false
}

func (p *emptyPredicate) Equals(other Predicate) bool {
	_, ok := other.(*emptyPredicate)
	return ok
}

func NewDomainIDPredicate(domainIDs []string, isExclusive bool) Predicate {
	domainIDSet := make(map[string]struct{})
	for _, domainID := range domainIDs {
		domainIDSet[domainID] = struct{}{}
	}
	return &domainIDPredicate{
		domainIDs:   domainIDSet,
		isExclusive: isExclusive,
	}
}

func (p *domainIDPredicate) IsEmpty() bool {
	return len(p.domainIDs) == 0 && !p.isExclusive
}

func (p *domainIDPredicate) Check(task persistence.Task) bool {
	if _, ok := p.domainIDs[task.GetDomainID()]; ok {
		return !p.isExclusive
	}
	return p.isExclusive
}

func (p *domainIDPredicate) Equals(other Predicate) bool {
	o, ok := other.(*domainIDPredicate)
	if !ok {
		return false
	}
	return p.isExclusive == o.isExclusive && maps.Equal(p.domainIDs, o.domainIDs)
}
