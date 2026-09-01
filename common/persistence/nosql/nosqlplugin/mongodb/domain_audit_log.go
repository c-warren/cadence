package mongodb

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// InsertDomainAuditLog inserts a new audit log entry for a domain operation
func (db *mdb) InsertDomainAuditLog(ctx context.Context, row *nosqlplugin.DomainAuditLogRow) error {
	return fmt.Errorf("InsertDomainAuditLog not implemented")
}

// SelectDomainAuditLogs returns audit log entries for a domain and operation type
func (db *mdb) SelectDomainAuditLogs(ctx context.Context, filter *nosqlplugin.DomainAuditLogFilter) ([]*nosqlplugin.DomainAuditLogRow, []byte, error) {
	return nil, nil, fmt.Errorf("SelectDomainAuditLogs not implemented")
}
