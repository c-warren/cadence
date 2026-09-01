package lib

import (
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/cadence/common/log"
)

const (
	defaultCadenceLocalHostPort = "127.0.0.1:7933"
	defaultCadenceServiceName   = "cadence-frontend"
)

// ContextKey is an alias for string, used as context key
type ContextKey string

const (
	// CtxKeyRuntimeContext is the name of the context key whose value is the RuntimeContext
	CtxKeyRuntimeContext = ContextKey("ctxKeyRuntimeCtx")

	// CtxKeyCadenceClient is the name of the context key for the cadence client this cadence worker listens to
	CtxKeyCadenceClient = ContextKey("ctxKeyCadenceClient")
)

// RuntimeContext contains all of the context information
// needed at cadence bench runtime
type RuntimeContext struct {
	Bench   Bench
	Cadence Cadence
	Logger  *zap.Logger
	Metrics tally.Scope
}

// NewRuntimeContext builds a runtime context from the config
func NewRuntimeContext(cfg *Config) (*RuntimeContext, error) {
	logger, err := cfg.Log.NewZapLogger()
	if err != nil {
		return nil, err
	}

	metricsScope := cfg.Metrics.NewScope(log.NewLogger(logger), cfg.Bench.Name)

	if cfg.Cadence.ServiceName == "" {
		cfg.Cadence.ServiceName = defaultCadenceServiceName
	}

	if cfg.Cadence.HostNameAndPort == "" {
		cfg.Cadence.HostNameAndPort = defaultCadenceLocalHostPort
	}

	return &RuntimeContext{
		Bench:   cfg.Bench,
		Cadence: cfg.Cadence,
		Logger:  logger,
		Metrics: metricsScope,
	}, nil
}
