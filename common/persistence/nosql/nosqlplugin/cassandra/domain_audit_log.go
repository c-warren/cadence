package cassandra

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

const (
	templateInsertDomainAuditLogQuery = `INSERT INTO domain_audit_log (` +
		`domain_id, event_id, state_before, state_before_encoding, state_after, state_after_encoding, ` +
		`operation_type, created_time, last_updated_time, identity, identity_type, comment) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateSelectDomainAuditLogsQuery = `SELECT ` +
		`event_id, domain_id, state_before, state_before_encoding, state_after, state_after_encoding, ` +
		`operation_type, created_time, last_updated_time, identity, identity_type, comment ` +
		`FROM domain_audit_log ` +
		`WHERE domain_id = ? AND operation_type = ? ` +
		`AND created_time >= ? AND created_time < ?`
)

// InsertDomainAuditLog inserts a new audit log entry for a domain operation
func (db *CDB) InsertDomainAuditLog(ctx context.Context, row *nosqlplugin.DomainAuditLogRow) error {
	query := db.session.Query(templateInsertDomainAuditLogQuery,
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
		row.TTLSeconds,
	).WithContext(ctx)

	err := query.Exec()
	return err
}

// SelectDomainAuditLogs returns audit log entries for a domain and operation type
func (db *CDB) SelectDomainAuditLogs(ctx context.Context, filter *nosqlplugin.DomainAuditLogFilter) ([]*nosqlplugin.DomainAuditLogRow, []byte, error) {
	if filter.MinCreatedTime == nil || filter.MaxCreatedTime == nil {
		return nil, nil, &types.InternalServiceError{
			Message: "SelectDomainAuditLogs requires non-nil MinCreatedTime and MaxCreatedTime",
		}
	}

	query := db.session.Query(templateSelectDomainAuditLogsQuery,
		filter.DomainID,
		filter.OperationType,
		*filter.MinCreatedTime,
		*filter.MaxCreatedTime,
	).WithContext(ctx)

	// Set page size
	if filter.PageSize > 0 {
		query = query.PageSize(filter.PageSize)
	}

	// Set page state for pagination
	if len(filter.NextPageToken) > 0 {
		query = query.PageState(filter.NextPageToken)
	}

	iter := query.Iter()
	if iter == nil {
		return nil, nil, &types.InternalServiceError{
			Message: "SelectDomainAuditLogs operation failed. Not able to create query iterator.",
		}
	}

	var rows []*nosqlplugin.DomainAuditLogRow
	row := &nosqlplugin.DomainAuditLogRow{}

	for iter.Scan(
		&row.EventID,
		&row.DomainID,
		&row.StateBefore,
		&row.StateBeforeEncoding,
		&row.StateAfter,
		&row.StateAfterEncoding,
		&row.OperationType,
		&row.CreatedTime,
		&row.LastUpdatedTime,
		&row.Identity,
		&row.IdentityType,
		&row.Comment,
	) {
		rows = append(rows, row)
		row = &nosqlplugin.DomainAuditLogRow{}

		// Break after collecting PageSize number of rows
		if filter.PageSize > 0 && len(rows) >= filter.PageSize {
			break
		}
	}

	nextPageToken := iter.PageState()
	if err := iter.Close(); err != nil {
		return nil, nil, err
	}

	return rows, nextPageToken, nil
}
