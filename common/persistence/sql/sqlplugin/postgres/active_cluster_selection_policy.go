package postgres

import (
	"context"
	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	selectActiveClusterSelectionPolicyQry = `
	SELECT shard_id, domain_id, workflow_id, run_id, data, data_encoding
	FROM active_cluster_selection_policy
	WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4
	`

	insertActiveClusterSelectionPolicyQry = `
	INSERT INTO active_cluster_selection_policy (shard_id, domain_id, workflow_id, run_id, data, data_encoding)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (shard_id, domain_id, workflow_id, run_id) DO NOTHING
	`

	deleteActiveClusterSelectionPolicyQry = `
	DELETE FROM active_cluster_selection_policy
	WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4
	`
)

func (pdb *db) InsertIntoActiveClusterSelectionPolicy(ctx context.Context, row *sqlplugin.ActiveClusterSelectionPolicyRow) (sql.Result, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(row.ShardID, pdb.GetTotalNumDBShards())
	return pdb.driver.ExecContext(ctx, dbShardID, insertActiveClusterSelectionPolicyQry, row.ShardID, row.DomainID, row.WorkflowID, row.RunID, row.Data, row.DataEncoding)
}

func (pdb *db) SelectFromActiveClusterSelectionPolicy(ctx context.Context, filter *sqlplugin.ActiveClusterSelectionPolicyFilter) (*sqlplugin.ActiveClusterSelectionPolicyRow, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(filter.ShardID, pdb.GetTotalNumDBShards())
	var row sqlplugin.ActiveClusterSelectionPolicyRow
	err := pdb.driver.GetContext(ctx, dbShardID, &row, selectActiveClusterSelectionPolicyQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (pdb *db) DeleteFromActiveClusterSelectionPolicy(ctx context.Context, filter *sqlplugin.ActiveClusterSelectionPolicyFilter) (sql.Result, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(filter.ShardID, pdb.GetTotalNumDBShards())
	return pdb.driver.ExecContext(ctx, dbShardID, deleteActiveClusterSelectionPolicyQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}
