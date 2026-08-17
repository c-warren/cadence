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
	"strings"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	now := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	dir := initTestRepo(t)

	tests := []struct {
		name     string
		fetch    TimeFetcher
		wantCode int
		wantOut  string
		wantErr  string
	}{
		{
			name: "young introduced version violates",
			fetch: func(_ context.Context, m, _ string) (time.Time, bool, error) {
				if m == "a.com/x" {
					return now.AddDate(0, 0, -3), true, nil
				}
				return now.AddDate(0, 0, -100), true, nil
			},
			wantCode: 1,
			wantOut:  "VIOLATION a.com/x@v1.1.0",
		},
		{
			name: "all old passes",
			fetch: func(_ context.Context, _, _ string) (time.Time, bool, error) {
				return now.AddDate(0, 0, -100), true, nil
			},
			wantCode: 0,
			wantOut:  "Checked 2 introduced dependency version(s); 0 violation(s).",
		},
		{
			name: "proxy failure exits 2",
			fetch: func(_ context.Context, _, _ string) (time.Time, bool, error) {
				return time.Time{}, false, errors.New("boom")
			},
			wantCode: 2,
			wantErr:  "ERROR module proxy unavailable, failing closed:",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out, errW bytes.Buffer
			code := Run(context.Background(), Config{
				BaseRef:       "main",
				ThresholdDays: 14,
				Fetch:         tc.fetch,
				Source:        &GitSource{Dir: dir},
				Now:           now,
				Out:           &out,
				Err:           &errW,
			})
			if code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr: %s)", code, tc.wantCode, errW.String())
			}
			if tc.wantOut != "" && !strings.Contains(out.String(), tc.wantOut) {
				t.Fatalf("stdout %q does not contain %q", out.String(), tc.wantOut)
			}
			if tc.wantErr != "" && !strings.Contains(errW.String(), tc.wantErr) {
				t.Fatalf("stderr %q does not contain %q", errW.String(), tc.wantErr)
			}
		})
	}
}
