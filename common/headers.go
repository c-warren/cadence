package common

import "github.com/uber/cadence/common/types"

const (
	// LibraryVersionHeaderName refers to the name of the
	// tchannel / http header that contains the client
	// library version
	LibraryVersionHeaderName = "cadence-client-library-version"

	// FeatureVersionHeaderName refers to the name of the
	// tchannel / http header that contains the client
	// feature version
	// the feature version sent from client represents the
	// feature set of the cadence client library support.
	// This can be used for client capibility check, on
	// Cadence server, for backward compatibility
	FeatureVersionHeaderName = "cadence-client-feature-version"

	// Header name to pass the feature flags from customer to client or server
	ClientFeatureFlagsHeaderName = "cadence-client-feature-flags"

	// ClientImplHeaderName refers to the name of the
	// header that contains the client implementation
	ClientImplHeaderName = "cadence-client-name"
	// AuthorizationTokenHeaderName refers to the jwt token in the request
	AuthorizationTokenHeaderName = "cadence-authorization"

	// AutoforwardingClusterHeaderName refers to the name of the header that contains the source cluster of the auto-forwarding request
	AutoforwardingClusterHeaderName = "cadence-forwarding-cluster"

	// PartitionConfigHeaderName refers to the name of the header that contains the json encoded partition config of the request
	PartitionConfigHeaderName = "cadence-workflow-partition-config"
	// IsolationGroupHeaderName refers to the name of the header that contains the isolation group of the client
	IsolationGroupHeaderName = "cadence-worker-isolation-group"

	// ClientIsolationGroupHeaderName refers to the name of the header that contains the isolation group which the client request is from
	ClientIsolationGroupHeaderName = "cadence-client-isolation-group"

	// CallerTypeHeaderName refers to the name of the header that contains the caller type (CLI, UI, SDK, internal, etc.)
	CallerTypeHeaderName = types.CallerTypeHeaderName
)
