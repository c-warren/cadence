package domain

import "github.com/uber/cadence/common/types"

var (
	// err indicating that this cluster is not the primary, so cannot do domain registration or update
	errNotPrimaryCluster                      = &types.BadRequestError{Message: "Cluster is not primary cluster, cannot do domain registration or domain update."}
	errCannotRemoveClustersFromDomain         = &types.BadRequestError{Message: "Cannot remove existing replicated clusters from a domain."}
	errActiveClusterNotInClusters             = &types.BadRequestError{Message: "Active cluster is not contained in all clusters."}
	errCannotDoDomainFailoverAndUpdate        = &types.BadRequestError{Message: "Cannot set active cluster to current cluster when other parameters are set."}
	errCannotDoGracefulFailoverFromCluster    = &types.BadRequestError{Message: "Cannot start the graceful failover from a to-be-passive cluster."}
	errGracefulFailoverInActiveCluster        = &types.BadRequestError{Message: "Cannot start the graceful failover from an active cluster to an active cluster."}
	errOngoingGracefulFailover                = &types.BadRequestError{Message: "Cannot start concurrent graceful failover."}
	errInvalidGracefulFailover                = &types.BadRequestError{Message: "Cannot start graceful failover without updating active cluster or in local domain."}
	errInvalidFailoverNoChangeDetected        = &types.BadRequestError{Message: "a failover was requested, but there was no change detected, the configuration was not updated"}
	errActiveClusterNameRequired              = &types.BadRequestError{Message: "ActiveClusterName is required for all global domains."}
	errLocalDomainsCannotFailover             = &types.BadRequestError{Message: "Local domains cannot perform failovers or change replication configuration"}
	errCannotPromoteToActiveActiveViaFailover = &types.BadRequestError{Message: "FailoverDomain cannot be used to promote a domain to active-active; use UpdateDomain to set cluster attributes"}

	errInvalidRetentionPeriod = &types.BadRequestError{Message: "A valid retention period is not set on request."}
	errInvalidArchivalConfig  = &types.BadRequestError{Message: "Invalid to enable archival without specifying a uri."}
)
