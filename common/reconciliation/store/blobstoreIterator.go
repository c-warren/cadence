package store

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/reconciliation/entity"
)

type (
	blobstoreIterator struct {
		itr pagination.Iterator
	}
)

// NewBlobstoreIterator constructs a new iterator backed by blobstore.
func NewBlobstoreIterator(
	ctx context.Context,
	client blobstore.Client,
	keys Keys,
	entity entity.Entity,
) ScanOutputIterator {
	return &blobstoreIterator{
		itr: pagination.NewIterator(ctx, keys.MinPage, getBlobstoreFetchPageFn(client, keys, entity)),
	}
}

// Next returns the next ScanOutputEntity
func (i *blobstoreIterator) Next() (*ScanOutputEntity, error) {
	exec, err := i.itr.Next()
	if exec != nil {
		return exec.(*ScanOutputEntity), err
	}
	return nil, err
}

// HasNext returns true if there is a next ScanOutputEntity false otherwise
func (i *blobstoreIterator) HasNext() bool {
	return i.itr.HasNext()
}

func getBlobstoreFetchPageFn(
	client blobstore.Client,
	keys Keys,
	entity entity.Entity,
) pagination.FetchFn {
	return func(ctx context.Context, token pagination.PageToken) (pagination.Page, error) {
		index := token.(int)
		key := pageNumberToKey(keys.UUID, keys.Extension, index)
		req := &blobstore.GetRequest{
			Key: key,
		}
		resp, err := client.Get(ctx, req)
		if err != nil {
			return pagination.Page{}, err
		}
		parts := bytes.Split(resp.Blob.Body, SeparatorToken)
		var executions []pagination.Entity
		for _, p := range parts {
			if len(p) == 0 {
				continue
			}
			soe, err := deserialize(p, entity)
			if err != nil {
				return pagination.Page{}, err
			}
			executions = append(executions, soe)
		}
		var nextPageToken interface{} = index + 1
		if nextPageToken.(int) > keys.MaxPage {
			nextPageToken = nil
		}
		return pagination.Page{
			CurrentToken: token,
			NextToken:    nextPageToken,
			Entities:     executions,
		}, nil
	}
}

func deserialize(data []byte, blob entity.Entity) (*ScanOutputEntity, error) {
	soe := &ScanOutputEntity{
		Execution: blob.Clone(),
	}

	if err := json.Unmarshal(data, soe); err != nil {
		return nil, err
	}

	if err := soe.Execution.(entity.Entity).Validate(); err != nil {
		return nil, err
	}
	return soe, nil
}
