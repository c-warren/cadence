package log

import (
	"fmt"
	"math/rand"

	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/log/tag"
)

type replayLogger struct {
	logger            Logger
	ctx               workflow.Context
	enableLogInReplay bool
}

const skipForReplayLogger = skipForDefaultLogger + 1

// NewReplayLogger creates a logger which is aware of cadence's replay mode
func NewReplayLogger(logger Logger, ctx workflow.Context, enableLogInReplay bool) Logger {
	lg, ok := logger.(*loggerImpl)
	if ok {
		logger = &loggerImpl{
			zapLogger:     lg.zapLogger,
			skip:          skipForReplayLogger,
			sampleLocalFn: lg.sampleLocalFn,
		}
	} else {
		logger.Warn("ReplayLogger may not emit callat tag correctly because the logger passed in is not loggerImpl")
	}
	return &replayLogger{
		logger:            logger,
		ctx:               ctx,
		enableLogInReplay: enableLogInReplay,
	}
}

func (r *replayLogger) Debugf(msg string, args ...any) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}

	r.logger.Debugf(fmt.Sprintf(msg, args...))
}

func (r *replayLogger) Debug(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Debug(msg, tags...)
}

func (r *replayLogger) Info(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Info(msg, tags...)
}

func (r *replayLogger) Warn(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Warn(msg, tags...)
}

func (r *replayLogger) Error(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Error(msg, tags...)
}

func (r *replayLogger) Fatal(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Fatal(msg, tags...)
}

func (r *replayLogger) SampleInfo(msg string, sampleRate int, tags ...tag.Tag) {
	if rand.Intn(sampleRate) == 0 {
		if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
			return
		}
		r.logger.Info(msg, tags...)
	}
}

func (r *replayLogger) DebugOn() bool {
	return r.logger.DebugOn()
}

func (r *replayLogger) WithTags(tags ...tag.Tag) Logger {
	return &replayLogger{
		logger:            r.logger.WithTags(tags...),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}

func (r *replayLogger) Helper() Logger {
	return &replayLogger{
		logger:            r.logger.Helper(),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}
