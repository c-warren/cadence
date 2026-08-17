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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultProxyRetries = 3
	defaultProxyBackoff = 2 * time.Second
	// The proxy synchronously fetches uncached versions from their origin, which
	// can exceed 10 seconds before returning a legitimate response such as 404.
	defaultProxyTimeout = 30 * time.Second
)

// ProxyClient queries a Go module proxy for version publish times.
type ProxyClient struct {
	BaseURL    string
	HTTPClient *http.Client
	Retries    int
	Backoff    time.Duration
}

// PublishTime returns the proxy publish time for a module version. Versions
// unknown to the proxy are reported with found=false. Proxy failures are
// retried and returned as errors after all attempts are exhausted.
func (c *ProxyClient) PublishTime(
	ctx context.Context,
	module string,
	version string,
) (time.Time, bool, error) {
	retries := c.Retries
	if retries == 0 {
		retries = defaultProxyRetries
	}
	backoff := c.Backoff
	if backoff == 0 {
		backoff = defaultProxyBackoff
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultProxyTimeout}
	}

	url := fmt.Sprintf(
		"%s/%s/@v/%s.info",
		strings.TrimRight(c.BaseURL, "/"),
		EscapeModulePath(module),
		EscapeVersion(version),
	)

	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		published, found, retry, err := fetchProxyTime(ctx, client, url)
		if !retry {
			return published, found, err
		}
		lastErr = err

		if attempt < retries {
			if err := waitForProxyRetry(ctx, backoff); err != nil {
				return time.Time{}, false, err
			}
		}
	}

	return time.Time{}, false, fmt.Errorf(
		"failed to query %s after %d attempt(s): %w",
		url,
		retries,
		lastErr,
	)
}

func fetchProxyTime(
	ctx context.Context,
	client *http.Client,
	url string,
) (published time.Time, found bool, retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return time.Time{}, false, true, fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return time.Time{}, false, false, ctx.Err()
		}
		return time.Time{}, false, true, fmt.Errorf("send request: %w", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return time.Time{}, false, true, fmt.Errorf("read response: %w", readErr)
	}
	if closeErr != nil {
		return time.Time{}, false, true, fmt.Errorf("close response: %w", closeErr)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		var info struct {
			Time json.RawMessage `json:"Time"`
		}
		if err := json.Unmarshal(body, &info); err != nil {
			return time.Time{}, false, true, fmt.Errorf("decode response: %w", err)
		}
		if len(info.Time) == 0 {
			return time.Time{}, false, false, nil
		}
		var timestamp string
		if err := json.Unmarshal(info.Time, &timestamp); err != nil || timestamp == "" {
			return time.Time{}, false, false, nil
		}
		published, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return time.Time{}, false, false, nil
		}
		return published, true, false, nil
	case http.StatusNotFound, http.StatusGone:
		return time.Time{}, false, false, nil
	default:
		return time.Time{}, false, true, fmt.Errorf("unexpected HTTP status %s", resp.Status)
	}
}

func waitForProxyRetry(ctx context.Context, backoff time.Duration) error {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
