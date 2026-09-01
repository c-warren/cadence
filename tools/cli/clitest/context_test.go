package clitest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
)

func TestNewCLIContext(t *testing.T) {
	app := cli.NewApp()
	ctx := NewCLIContext(
		t,
		app,
		StringArgument("region", "dca"),
		IntArgument("shards", 1024),
		BoolArgument("verbose", true),
		BoolArgument("exit-if-error", false),
		Int64Argument("bytes-per-minute", 999876543210),
		StringSliceArgument("tags", "tag1", "tag2"),
	)

	assert.True(t, ctx.IsSet("region"))
	assert.Equal(t, "dca", ctx.String("region"))

	assert.True(t, ctx.IsSet("shards"))
	assert.Equal(t, 1024, ctx.Int("shards"))

	assert.True(t, ctx.IsSet("verbose"))
	assert.True(t, ctx.Bool("verbose"))

	assert.True(t, ctx.IsSet("exit-if-error"))
	assert.False(t, ctx.Bool("exit-if-error"))

	assert.True(t, ctx.IsSet("bytes-per-minute"))
	assert.Equal(t, int64(999876543210), ctx.Int64("bytes-per-minute"))

	assert.True(t, ctx.IsSet("tags"))
	assert.Equal(t, []string{"tag1", "tag2"}, ctx.StringSlice("tags"))

	assert.False(t, ctx.IsSet("should-not-exist"))
}
