package isolationgroup

import "context"

const (
	GroupKey         = "isolation-group"
	OriginalGroupKey = "original-isolation-group"
	WorkflowIDKey    = "wf-id"
)

type configKey struct{}

type isolationGroupKey struct{}

// ConfigFromContext retrieves infomation about the partition config of the context
// which is used for tasklist isolation
func ConfigFromContext(ctx context.Context) map[string]string {
	val, ok := ctx.Value(configKey{}).(map[string]string)
	if !ok {
		return nil
	}
	return val
}

// ContextWithConfig stores the partition config of tasklist isolation into the given context
func ContextWithConfig(ctx context.Context, partitionConfig map[string]string) context.Context {
	return context.WithValue(ctx, configKey{}, partitionConfig)
}

// IsolationGroupFromContext retrieves the isolation group from the given context,
// which is used to identify which isolation group the poller is from
func IsolationGroupFromContext(ctx context.Context) string {
	val, ok := ctx.Value(isolationGroupKey{}).(string)
	if !ok {
		return ""
	}
	return val
}

// ContextWithIsolationGroup stores the isolation group into the given context
func ContextWithIsolationGroup(ctx context.Context, isolationGroup string) context.Context {
	return context.WithValue(ctx, isolationGroupKey{}, isolationGroup)
}
