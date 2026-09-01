package dynamodb

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// InsertHistoryDLQTaskRow writes a task to the history DLQ.
func (db *ddb) InsertHistoryDLQTaskRow(ctx context.Context, task *nosqlplugin.HistoryDLQTaskRow) error {
	return fmt.Errorf("InsertHistoryDLQTaskRow not implemented for DynamoDB")
}

func (db *ddb) SelectHistoryDLQTaskRows(ctx context.Context, filter nosqlplugin.HistoryDLQTaskFilter) ([]*nosqlplugin.HistoryDLQTaskRow, []byte, error) {
	return nil, nil, fmt.Errorf("SelectHistoryDLQTaskRows not implemented for DynamoDB")
}

func (db *ddb) RangeDeleteHistoryDLQTaskRows(ctx context.Context, filter nosqlplugin.HistoryDLQTaskRangeDeleteFilter) error {
	return fmt.Errorf("RangeDeleteHistoryDLQTaskRows not implemented for DynamoDB")
}

func (db *ddb) SelectHistoryDLQAckLevelRows(ctx context.Context, filter nosqlplugin.HistoryDLQAckLevelFilter) ([]*nosqlplugin.HistoryDLQAckLevelRow, error) {
	return nil, fmt.Errorf("SelectHistoryDLQAckLevelRows not implemented for DynamoDB")
}

func (db *ddb) InsertOrUpdateHistoryDLQAckLevelRow(ctx context.Context, row *nosqlplugin.HistoryDLQAckLevelRow) error {
	return fmt.Errorf("InsertOrUpdateHistoryDLQAckLevelRow not implemented for DynamoDB")
}

func (db *ddb) InsertHistoryDLQAckLevelIfNotExistsRow(ctx context.Context, row *nosqlplugin.HistoryDLQAckLevelRow) error {
	return fmt.Errorf("InsertHistoryDLQAckLevelIfNotExistsRow not implemented for DynamoDB")
}
