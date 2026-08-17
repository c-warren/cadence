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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/uber/cadence/tools/dependencyage"
)

const (
	defaultThresholdDays = 14
	defaultProxyURL      = "https://proxy.golang.org"
)

func main() {
	baseRef := flag.String("base-ref", "", "git ref to diff against (e.g. origin/master)")
	flag.Parse()
	if strings.TrimSpace(*baseRef) == "" {
		flag.Usage()
		os.Exit(2)
	}

	rawThreshold := strings.TrimSpace(os.Getenv("MIN_DEPENDENCY_AGE_DAYS"))
	thresholdDays := defaultThresholdDays
	if rawThreshold != "" {
		var err error
		thresholdDays, err = strconv.Atoi(rawThreshold)
		if err != nil {
			_, _ = fmt.Fprintf(
				os.Stderr,
				"ERROR MIN_DEPENDENCY_AGE_DAYS must be an integer, got %q\n",
				rawThreshold,
			)
			os.Exit(2)
		}
	}

	proxyURL := os.Getenv("DEP_AGE_PROXY_URL")
	if proxyURL == "" {
		proxyURL = defaultProxyURL
	}
	proxy := &dependencyage.ProxyClient{BaseURL: proxyURL}

	os.Exit(dependencyage.Run(context.Background(), dependencyage.Config{
		BaseRef:       *baseRef,
		ThresholdDays: thresholdDays,
		Fetch:         proxy.PublishTime,
		Source:        &dependencyage.GitSource{},
		Now:           time.Now().UTC(),
		Out:           os.Stdout,
		Err:           os.Stderr,
	}))
}
