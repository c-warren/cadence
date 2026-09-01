package dynamodb

import (
	"context"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// Insert message into queue, return error if failed or already exists
// Return ConditionFailure if the condition doesn't meet
func (db *ddb) InsertIntoQueue(
	ctx context.Context,
	row *nosqlplugin.QueueMessageRow,
) error {
	panic("TODO")
}

// Get the ID of last message inserted into the queue
func (db *ddb) SelectLastEnqueuedMessageID(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {
	panic("TODO")
}

// Read queue messages starting from the exclusiveBeginMessageID
func (db *ddb) SelectMessagesFrom(
	ctx context.Context,
	queueType persistence.QueueType,
	exclusiveBeginMessageID int64,
	maxRows int,
) ([]*nosqlplugin.QueueMessageRow, error) {
	panic("TODO")
}

// Read queue message starting from exclusiveBeginMessageID int64, inclusiveEndMessageID int64
func (db *ddb) SelectMessagesBetween(
	ctx context.Context,
	request nosqlplugin.SelectMessagesBetweenRequest,
) (*nosqlplugin.SelectMessagesBetweenResponse, error) {
	panic("TODO")
}

// Delete all messages before exclusiveBeginMessageID
func (db *ddb) DeleteMessagesBefore(
	ctx context.Context,
	queueType persistence.QueueType,
	exclusiveBeginMessageID int64,
) error {
	panic("TODO")
}

// Delete all messages in a range between exclusiveBeginMessageID and inclusiveEndMessageID
func (db *ddb) DeleteMessagesInRange(
	ctx context.Context,
	queueType persistence.QueueType,
	exclusiveBeginMessageID int64,
	inclusiveEndMessageID int64,
) error {
	panic("TODO")
}

// Delete one message
func (db *ddb) DeleteMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	panic("TODO")
}

// Insert an empty metadata row, starting from a version
func (db *ddb) InsertQueueMetadata(ctx context.Context, row nosqlplugin.QueueMetadataRow) error {
	panic("TODO")
}

// **Conditionally** update a queue metadata row, if current version is matched(meaning current == row.Version - 1),
// then the current version will increase by one when updating the metadata row
// Return ConditionFailure if the condition doesn't meet
func (db *ddb) UpdateQueueMetadataCas(
	ctx context.Context,
	row nosqlplugin.QueueMetadataRow,
) error {
	panic("TODO")
}

// Read a QueueMetadata
func (db *ddb) SelectQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueType,
) (*nosqlplugin.QueueMetadataRow, error) {
	panic("TODO")
}

func (db *ddb) GetQueueSize(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {
	panic("TODO")
}
