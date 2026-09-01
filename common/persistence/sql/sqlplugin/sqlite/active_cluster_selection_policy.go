package sqlite

import (
	"context"
	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	// Insert is silently skipped if a row with the same (shard_id, domain_id, workflow_id, run_id) primary key already exists
	insertActiveClusterSelectionPolicyQuery = `INSERT OR IGNORE INTO active_cluster_selection_policy
		(shard_id, domain_id, workflow_id, run_id, data, data_encoding)
		VALUES (?, ?, ?, ?, ?, ?)`

	getActiveClusterSelectionPolicyQuery = `SELECT shard_id, domain_id, workflow_id, run_id, data, data_encoding
		FROM active_cluster_selection_policy
		WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?`

	deleteActiveClusterSelectionPolicyQuery = `DELETE FROM active_cluster_selection_policy
		WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?`
)

func (mdb *DB) InsertIntoActiveClusterSelectionPolicy(ctx context.Context, row *sqlplugin.ActiveClusterSelectionPolicyRow) (sql.Result, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(row.ShardID, mdb.GetTotalNumDBShards())
	return mdb.driver.ExecContext(
		ctx,
		dbShardID,
		insertActiveClusterSelectionPolicyQuery,
		row.ShardID,
		row.DomainID,
		row.WorkflowID,
		row.RunID,
		row.Data,
		row.DataEncoding,
	)
}

func (mdb *DB) SelectFromActiveClusterSelectionPolicy(ctx context.Context, filter *sqlplugin.ActiveClusterSelectionPolicyFilter) (*sqlplugin.ActiveClusterSelectionPolicyRow, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(filter.ShardID, mdb.GetTotalNumDBShards())
	var row sqlplugin.ActiveClusterSelectionPolicyRow
	err := mdb.driver.GetContext(
		ctx,
		dbShardID,
		&row,
		getActiveClusterSelectionPolicyQuery,
		filter.ShardID,
		filter.DomainID,
		filter.WorkflowID,
		filter.RunID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *DB) DeleteFromActiveClusterSelectionPolicy(ctx context.Context, filter *sqlplugin.ActiveClusterSelectionPolicyFilter) (sql.Result, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(filter.ShardID, mdb.GetTotalNumDBShards())
	return mdb.driver.ExecContext(
		ctx,
		dbShardID,
		deleteActiveClusterSelectionPolicyQuery,
		filter.ShardID,
		filter.DomainID,
		filter.WorkflowID,
		filter.RunID,
	)
}
