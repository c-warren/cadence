package dynamodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// InsertDomainAuditLog inserts a new audit log entry for a domain operation
func (db *ddb) InsertDomainAuditLog(ctx context.Context, row *nosqlplugin.DomainAuditLogRow) error {
	panic("TODO: InsertDomainAuditLog not implemented")
}

// SelectDomainAuditLogs returns audit log entries for a domain and operation type
func (db *ddb) SelectDomainAuditLogs(ctx context.Context, filter *nosqlplugin.DomainAuditLogFilter) ([]*nosqlplugin.DomainAuditLogRow, []byte, error) {
	panic("TODO: SelectDomainAuditLogs not implemented")
}
