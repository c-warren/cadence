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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyClientPublishTime(t *testing.T) {
	ctx := context.Background()

	t.Run("success parses time and escapes path", func(t *testing.T) {
		var gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			w.Write([]byte(`{"Version":"v1.38.1","Time":"2023-03-07T22:20:22Z"}`))
		}))
		defer srv.Close()
		c := &ProxyClient{BaseURL: srv.URL}
		got, found, err := c.PublishTime(ctx, "github.com/Shopify/sarama", "v1.38.1")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, time.Date(2023, 3, 7, 22, 20, 22, 0, time.UTC), got.UTC())
		assert.Equal(t, "/github.com/!shopify/sarama/@v/v1.38.1.info", gotPath)
	})

	t.Run("404 means not found without error", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()
		c := &ProxyClient{BaseURL: srv.URL}
		_, found, err := c.PublishTime(ctx, "a.com/x", "v1.0.0")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("500 retries then fails closed", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		c := &ProxyClient{BaseURL: srv.URL, Retries: 2, Backoff: time.Millisecond}
		_, _, err := c.PublishTime(ctx, "a.com/x", "v1.0.0")
		require.Error(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("transport error fails closed", func(t *testing.T) {
		c := &ProxyClient{BaseURL: "http://127.0.0.1:1", Retries: 1, Backoff: time.Millisecond}
		_, _, err := c.PublishTime(ctx, "a.com/x", "v1.0.0")
		require.Error(t, err)
	})
}
