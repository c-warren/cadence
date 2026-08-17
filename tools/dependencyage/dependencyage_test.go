// The MIT License (MIT)

// Copyright (c) 2026 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package dependencyage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const goModBase = `module github.com/uber/cadence

go 1.23

require (
	github.com/stretchr/testify v1.8.0
	go.uber.org/zap v1.24.0 // indirect
)

require github.com/Shopify/sarama v1.38.1
`

const goModHead = `module github.com/uber/cadence

go 1.23

require (
	github.com/stretchr/testify v1.9.0
	go.uber.org/zap v1.24.0 // indirect
	github.com/new/dep v0.1.0
)

require github.com/Shopify/sarama v1.38.1
`

const goModReplaceBase = `module github.com/uber/cadence

go 1.23

require github.com/foo/bar v1.0.0

replace github.com/foo/bar => github.com/foo/bar v1.0.1
`

const goModReplaceHead = `module github.com/uber/cadence

go 1.23

require github.com/foo/bar v1.0.0

replace github.com/foo/bar => github.com/evil/fork v0.0.0-20260812000000-abcdef123456

replace (
	github.com/baz/qux v2.0.0 => github.com/baz/qux v2.1.0
	github.com/local/dep => ../localdep
)
`

// Same fork target on both sides; only the left-hand version qualifier and
// the require version change. The target must NOT be reported as new.
const goModRepinBase = `module github.com/uber/cadence

go 1.23

require github.com/foo/bar v1.0.0

replace github.com/foo/bar v1.0.0 => github.com/fork/bar v5.0.0
`

const goModRepinHead = `module github.com/uber/cadence

go 1.23

require github.com/foo/bar v1.2.0

replace github.com/foo/bar v1.2.0 => github.com/fork/bar v5.0.0
`

// Constructs the canonical parser must handle that a regex parser may not.
const goModExotic = `module github.com/uber/cadence/v2

go 1.23

toolchain go1.23.4

require (
	github.com/major/mod/v3 v3.2.1
	"github.com/quoted/path" v1.0.0
)

retract v2.0.1

exclude github.com/bad/mod v0.9.0
`

func TestParseRequires(t *testing.T) {
	got, err := ParseRequires(goModBase)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"github.com/stretchr/testify": "v1.8.0",
		"go.uber.org/zap":             "v1.24.0",
		"github.com/Shopify/sarama":   "v1.38.1",
	}, got)
}

func TestParseRequiresExotic(t *testing.T) {
	got, err := ParseRequires(goModExotic)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"github.com/major/mod/v3": "v3.2.1",
		"github.com/quoted/path":  "v1.0.0",
	}, got)
}

func TestParseRequiresInvalid(t *testing.T) {
	_, err := ParseRequires("module \x00 not a gomod ((")
	require.Error(t, err)
}

func TestNewRequirements(t *testing.T) {
	tests := []struct {
		name string
		base string
		head string
		want []ModuleVersion
	}{
		{
			name: "bumped and new modules reported, unchanged not",
			base: goModBase,
			head: goModHead,
			want: []ModuleVersion{
				{"github.com/new/dep", "v0.1.0"},
				{"github.com/stretchr/testify", "v1.9.0"},
			},
		},
		{
			name: "empty base reports everything",
			base: "",
			head: goModBase,
			want: []ModuleVersion{
				{"github.com/Shopify/sarama", "v1.38.1"},
				{"github.com/stretchr/testify", "v1.8.0"},
				{"go.uber.org/zap", "v1.24.0"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewRequirements(tc.base, tc.head)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestParseReplaces(t *testing.T) {
	got, err := ParseReplaces(goModReplaceHead)
	require.NoError(t, err)
	assert.ElementsMatch(t, []Replacement{
		{
			Old: ModuleVersion{"github.com/foo/bar", ""},
			New: &ModuleVersion{"github.com/evil/fork", "v0.0.0-20260812000000-abcdef123456"},
		},
		{
			Old: ModuleVersion{"github.com/baz/qux", "v2.0.0"},
			New: &ModuleVersion{"github.com/baz/qux", "v2.1.0"},
		},
		{
			Old: ModuleVersion{"github.com/local/dep", ""},
			New: nil,
		},
	}, got)
}

func TestNewReplacements(t *testing.T) {
	tests := []struct {
		name string
		base string
		head string
		want []ModuleVersion
	}{
		{
			name: "changed and added replacements reported, filesystem skipped",
			base: goModReplaceBase,
			head: goModReplaceHead,
			want: []ModuleVersion{
				{"github.com/baz/qux", "v2.1.0"},
				{"github.com/evil/fork", "v0.0.0-20260812000000-abcdef123456"},
			},
		},
		{
			name: "unchanged replacement not reported",
			base: goModReplaceBase,
			head: goModReplaceBase,
			want: nil,
		},
		{
			name: "left-side version repin with identical target not reported",
			base: goModRepinBase,
			head: goModRepinHead,
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReplacements(tc.base, tc.head)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestEscaping(t *testing.T) {
	assert.Equal(t, "github.com/!shopify/sarama", EscapeModulePath("github.com/Shopify/sarama"))
	assert.Equal(t, "v1.0.0-!r!c1", EscapeVersion("v1.0.0-RC1"))
}

func TestPseudoVersionTime(t *testing.T) {
	got, ok := PseudoVersionTime("v0.0.0-20240102150405-abcdef123456")
	require.True(t, ok)
	assert.Equal(t, time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC), got)

	_, ok = PseudoVersionTime("v1.2.3")
	assert.False(t, ok)
}

func TestFindViolations(t *testing.T) {
	now := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("young version violates, old version passes", func(t *testing.T) {
		times := map[ModuleVersion]time.Time{
			{"a.com/young", "v1.0.0"}: now.AddDate(0, 0, -5),
			{"a.com/old", "v1.0.0"}:   now.AddDate(0, 0, -20),
		}
		fetch := func(_ context.Context, m, v string) (time.Time, bool, error) {
			t, ok := times[ModuleVersion{m, v}]
			return t, ok, nil
		}
		got, err := FindViolations(ctx,
			[]ModuleVersion{{"a.com/young", "v1.0.0"}, {"a.com/old", "v1.0.0"}},
			14, now, fetch, &bytes.Buffer{})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, ModuleVersion{"a.com/young", "v1.0.0"}, got[0].ModuleVersion)
	})

	t.Run("unknown time warns but does not violate", func(t *testing.T) {
		var warn bytes.Buffer
		fetch := func(_ context.Context, _, _ string) (time.Time, bool, error) {
			return time.Time{}, false, nil
		}
		got, err := FindViolations(ctx,
			[]ModuleVersion{{"a.com/unknown", "v1.0.0"}}, 14, now, fetch, &warn)
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Contains(t, warn.String(), "WARN")
	})

	t.Run("pseudo-version fallback when fetcher has no time", func(t *testing.T) {
		fetch := func(_ context.Context, _, _ string) (time.Time, bool, error) {
			return time.Time{}, false, nil
		}
		got, err := FindViolations(ctx,
			[]ModuleVersion{{"a.com/pseudo", "v0.0.0-20260810000000-abcdef123456"}},
			14, now, fetch, &bytes.Buffer{})
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})

	t.Run("fetcher error fails closed", func(t *testing.T) {
		fetch := func(_ context.Context, _, _ string) (time.Time, bool, error) {
			return time.Time{}, false, errors.New("proxy unreachable")
		}
		_, err := FindViolations(ctx,
			[]ModuleVersion{{"a.com/x", "v1.0.0"}}, 14, now, fetch, &bytes.Buffer{})
		require.Error(t, err)
	})
}
