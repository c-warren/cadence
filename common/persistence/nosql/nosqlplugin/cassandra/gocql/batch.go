package gocql

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

var _ Batch = (*batch)(nil)

type (
	batch struct {
		*gocql.Batch
	}
)

// Definition of all BatchTypes
const (
	LoggedBatch BatchType = iota
	UnloggedBatch
	CounterBatch
)

func newBatch(
	gocqlBatch *gocql.Batch,
) *batch {
	return &batch{
		Batch: gocqlBatch,
	}
}

func (b *batch) WithContext(ctx context.Context) Batch {
	b2 := b.Batch.WithContext(ctx)
	if b2 == nil {
		return nil
	}
	return newBatch(b2)
}

func (b *batch) WithTimestamp(timestamp int64) Batch {
	b.Batch.WithTimestamp(timestamp)
	return b
}

func (b *batch) Consistency(c Consistency) Batch {
	b.Batch.SetConsistency(mustConvertConsistency(c))
	return b
}

func mustConvertBatchType(batchType BatchType) gocql.BatchType {
	switch batchType {
	case LoggedBatch:
		return gocql.LoggedBatch
	case UnloggedBatch:
		return gocql.UnloggedBatch
	case CounterBatch:
		return gocql.CounterBatch
	default:
		panic(fmt.Sprintf("Unknown gocql BatchType: %v", batchType))
	}
}
