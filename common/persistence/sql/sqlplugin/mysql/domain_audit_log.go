package mysql

import (
	"context"
	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	_insertDomainAuditLogQuery = `INSERT INTO domain_audit_log (
		domain_id, event_id, state_before, state_before_encoding, state_after, state_after_encoding,
		operation_type, created_time, last_updated_time, identity, identity_type, comment
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_selectDomainAuditLogsQuery = `SELECT
		event_id, domain_id, state_before, state_before_encoding, state_after, state_after_encoding,
		operation_type, created_time, last_updated_time, identity, identity_type, comment
	FROM domain_audit_log
	WHERE domain_id = ? AND operation_type = ? AND created_time >= ?
	AND (created_time < ? OR (created_time = ? AND event_id > ?))
	ORDER BY created_time DESC, event_id ASC
	LIMIT ?`
	_selectAllDomainAuditLogsQuery = `SELECT
		event_id, domain_id, state_before, state_before_encoding, state_after, state_after_encoding,
		operation_type, created_time, last_updated_time, identity, identity_type, comment
	FROM domain_audit_log
	WHERE domain_id = ? AND operation_type = ? AND created_time >= ?
	AND (created_time < ? OR (created_time = ? AND event_id > ?))
	ORDER BY created_time DESC, event_id ASC`
)

// InsertIntoDomainAuditLog inserts a single row into domain_audit_log table
func (mdb *DB) InsertIntoDomainAuditLog(ctx context.Context, row *sqlplugin.DomainAuditLogRow) (sql.Result, error) {
	return mdb.driver.ExecContext(
		ctx,
		sqlplugin.DbDefaultShard,
		_insertDomainAuditLogQuery,
		row.DomainID,
		row.EventID,
		row.StateBefore,
		row.StateBeforeEncoding,
		row.StateAfter,
		row.StateAfterEncoding,
		row.OperationType,
		row.CreatedTime,
		row.LastUpdatedTime,
		row.Identity,
		row.IdentityType,
		row.Comment,
	)
}

// SelectFromDomainAuditLogs returns audit log entries for a domain, operation type, and time range
func (mdb *DB) SelectFromDomainAuditLogs(
	ctx context.Context,
	filter *sqlplugin.DomainAuditLogFilter,
) ([]*sqlplugin.DomainAuditLogRow, error) {
	args := []interface{}{
		filter.DomainID,
		filter.OperationType,
		*filter.MinCreatedTime,
		*filter.PageMaxCreatedTime,
		*filter.PageMaxCreatedTime,
		*filter.PageMinEventID,
	}

	var rows []*sqlplugin.DomainAuditLogRow
	if filter.PageSize > 0 {
		args = append(args, filter.PageSize)
		err := mdb.driver.SelectContext(ctx, sqlplugin.DbDefaultShard, &rows, _selectDomainAuditLogsQuery, args...)
		if err != nil {
			return nil, err
		}
	} else {
		err := mdb.driver.SelectContext(ctx, sqlplugin.DbDefaultShard, &rows, _selectAllDomainAuditLogsQuery, args...)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}
