package service

import "strings"

const (
	_servicePrefix = "cadence-"

	// Frontend is the name of the frontend service
	Frontend = "cadence-frontend"
	// History is the name of the history service
	History = "cadence-history"
	// Matching is the name of the matching service
	Matching = "cadence-matching"
	// Worker is the name of the worker service
	Worker = "cadence-worker"
	// ShardDistributor is the name of the shard distributor service
	ShardDistributor = "shard-distributor"
)

// ListWithRing contains the list of all cadence services that has a hash ring
var ListWithRing = []string{Frontend, History, Matching, Worker}

// List contains the list of all cadence services
var List = []string{Frontend, History, Matching, Worker}

// ShortName returns cadence service name without "cadence-" prefix
func ShortName(name string) string {
	return strings.TrimPrefix(name, _servicePrefix)
}

// FullName returns cadence service name with "cadence-" prefix
func FullName(name string) string {
	if strings.HasPrefix(name, _servicePrefix) {
		return name
	}
	return _servicePrefix + name
}

func ShortNames(names []string) []string {
	result := make([]string, len(names))
	for i := range names {
		result[i] = ShortName(names[i])
	}
	return result
}
