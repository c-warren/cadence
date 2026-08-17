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

// require github.com/commented/out v9.9.9
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

func TestParseRequires(t *testing.T) {
	got := ParseRequires(goModBase)
	assert.Equal(t, map[string]string{
		"github.com/stretchr/testify": "v1.8.0",
		"go.uber.org/zap":             "v1.24.0",
		"github.com/Shopify/sarama":   "v1.38.1",
	}, got)
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
			assert.ElementsMatch(t, tc.want, NewRequirements(tc.base, tc.head))
		})
	}
}

func TestParseReplaces(t *testing.T) {
	got := ParseReplaces(goModReplaceHead)
	assert.Equal(t, map[string]*ModuleVersion{
		"github.com/foo/bar":        {"github.com/evil/fork", "v0.0.0-20260812000000-abcdef123456"},
		"github.com/baz/qux v2.0.0": {"github.com/baz/qux", "v2.1.0"},
		"github.com/local/dep":      nil,
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, NewReplacements(tc.base, tc.head))
		})
	}
}

func TestEscapeModulePath(t *testing.T) {
	assert.Equal(t, "github.com/!shopify/sarama", EscapeModulePath("github.com/Shopify/sarama"))
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
