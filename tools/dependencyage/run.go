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
	"context"
	"fmt"
	"io"
	"time"
)

// Config carries the inputs and outputs needed by Run.
type Config struct {
	BaseRef       string
	ThresholdDays int
	Fetch         TimeFetcher
	Source        *GitSource
	Now           time.Time
	Out           io.Writer
	Err           io.Writer
}

// Run executes the dependency age check and returns its process exit code.
func Run(ctx context.Context, cfg Config) int {
	out := cfg.Out
	if out == nil {
		out = io.Discard
	}
	errW := cfg.Err
	if errW == nil {
		errW = io.Discard
	}

	if cfg.Source == nil {
		_, _ = fmt.Fprintln(errW, "ERROR git source is required")
		return 2
	}
	if cfg.Fetch == nil {
		_, _ = fmt.Fprintln(errW, "ERROR module proxy fetcher is required")
		return 2
	}

	paths, err := cfg.Source.ChangedGoModFiles(cfg.BaseRef)
	if err != nil {
		_, _ = fmt.Fprintf(errW, "ERROR %v\n", err)
		return 2
	}

	pairSet := make(map[ModuleVersion]struct{})
	for _, path := range paths {
		head, base, _, err := cfg.Source.Contents(cfg.BaseRef, path)
		if err != nil {
			_, _ = fmt.Fprintf(errW, "ERROR %v\n", err)
			return 2
		}
		for _, pair := range NewRequirements(base, head) {
			pairSet[pair] = struct{}{}
		}
		for _, pair := range NewReplacements(base, head) {
			pairSet[pair] = struct{}{}
		}
	}

	pairs := make([]ModuleVersion, 0, len(pairSet))
	for pair := range pairSet {
		pairs = append(pairs, pair)
	}
	sortModuleVersions(pairs)

	violations, err := FindViolations(
		ctx,
		pairs,
		cfg.ThresholdDays,
		cfg.Now,
		cfg.Fetch,
		errW,
	)
	if err != nil {
		_, _ = fmt.Fprintf(
			errW,
			"ERROR module proxy unavailable, failing closed: %v\n",
			err,
		)
		return 2
	}

	for _, violation := range violations {
		daysAgo := int(cfg.Now.Sub(violation.Published) / (24 * time.Hour))
		_, _ = fmt.Fprintf(
			out,
			"VIOLATION %s@%s published %s (%d days ago, minimum %d)\n",
			violation.Module,
			violation.Version,
			violation.Published.Format(time.RFC3339),
			daysAgo,
			cfg.ThresholdDays,
		)
	}
	_, _ = fmt.Fprintf(
		out,
		"Checked %d introduced dependency version(s); %d violation(s).\n",
		len(pairs),
		len(violations),
	)

	if len(violations) > 0 {
		return 1
	}
	return 0
}
