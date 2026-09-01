package types

import "context"

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination persister_mock.go -package types github.com/uber/cadence/common/mapq/types Persister

type Persister interface {
	Persist(ctx context.Context, items []ItemToPersist) error
	GetOffsets(ctx context.Context) (*Offsets, error)
	CommitOffsets(ctx context.Context, offsets *Offsets) error
	Fetch(ctx context.Context, partitions ItemPartitions, pageInfo PageInfo) ([]Item, error)
}

type PageInfo struct {
	// TODO: define ack levels, page size, etc.
}
