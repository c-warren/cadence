package blobstore

import (
	"context"

	"github.com/uber/cadence/common/backoff"
)

type (
	retryableClient struct {
		client        Client
		throttleRetry *backoff.ThrottleRetry
	}
)

// NewRetryableClient constructs a blobstorre client which retries transient errors.
func NewRetryableClient(client Client, policy backoff.RetryPolicy) Client {
	return &retryableClient{
		client: client,
		throttleRetry: backoff.NewThrottleRetry(
			backoff.WithRetryPolicy(policy),
			backoff.WithRetryableError(client.IsRetryableError),
		),
	}
}

func (c *retryableClient) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	var resp *PutResponse
	var err error
	op := func(ctx context.Context) error {
		resp, err = c.client.Put(ctx, req)
		return err
	}
	err = c.throttleRetry.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	var resp *GetResponse
	var err error
	op := func(ctx context.Context) error {
		resp, err = c.client.Get(ctx, req)
		return err
	}
	err = c.throttleRetry.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) Exists(ctx context.Context, req *ExistsRequest) (*ExistsResponse, error) {
	var resp *ExistsResponse
	var err error
	op := func(ctx context.Context) error {
		resp, err = c.client.Exists(ctx, req)
		return err
	}
	err = c.throttleRetry.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	var resp *DeleteResponse
	var err error
	op := func(ctx context.Context) error {
		resp, err = c.client.Delete(ctx, req)
		return err
	}
	err = c.throttleRetry.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *retryableClient) IsRetryableError(err error) bool {
	return c.client.IsRetryableError(err)
}
