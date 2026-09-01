package clitest

import (
	"flag"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// CliArgument is a way to apply argument to given flagset/context
type CliArgument func(t *testing.T, flags *flag.FlagSet, c *cli.Context)

// StringArgument introduces a new string argument for cli context
func StringArgument(name, value string) CliArgument {
	return func(t *testing.T, flags *flag.FlagSet, c *cli.Context) {
		t.Helper()
		flags.String(name, "", "")
		require.NoError(t, c.Set(name, value))
	}
}

// IntArgument introduces a new int argument for cli context
func IntArgument(name string, value int) CliArgument {
	return func(t *testing.T, flags *flag.FlagSet, c *cli.Context) {
		t.Helper()
		flags.Int(name, 0, "")
		require.NoError(t, c.Set(name, strconv.Itoa(value)))
	}
}

// BoolArgument introduces a new boolean argument for cli context
func BoolArgument(name string, value bool) CliArgument {
	return func(t *testing.T, flags *flag.FlagSet, c *cli.Context) {
		t.Helper()
		flags.Bool(name, value, "")
		require.NoError(t, c.Set(name, strconv.FormatBool(value)))
	}
}

// Int64Argument introduces a new int64 argument for cli context
func Int64Argument(name string, value int64) CliArgument {
	return func(t *testing.T, flags *flag.FlagSet, c *cli.Context) {
		t.Helper()
		flags.Int64(name, value, "")
		require.NoError(t, c.Set(name, strconv.FormatInt(value, 10)))
	}
}

// StringSliceArgument introduces a new string slice argument for cli context
func StringSliceArgument(name string, values ...string) CliArgument {
	return func(t *testing.T, flags *flag.FlagSet, c *cli.Context) {
		t.Helper()
		flags.Var(&cli.StringSlice{}, name, "")
		for _, v := range values {
			require.NoError(t, c.Set(name, v))
		}
	}
}

// NewCLIContext creates a new cli context with optional arguments
// this is a useful to make testing of commands compact
func NewCLIContext(t *testing.T, app *cli.App, args ...CliArgument) *cli.Context {
	t.Helper()
	flags := flag.NewFlagSet("test", 0)
	cliCtx := cli.NewContext(app, flags, nil)
	for _, arg := range args {
		arg(t, flags, cliCtx)
	}
	return cliCtx
}
