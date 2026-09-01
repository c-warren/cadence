package mongodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// Insert a new record to domain, return error if failed or already exists
// Return ConditionFailure if the condition doesn't meet
func (db *mdb) InsertDomain(
	ctx context.Context,
	row *nosqlplugin.DomainRow,
) error {
	panic("TODO")
}

// Update domain
func (db *mdb) UpdateDomain(
	ctx context.Context,
	row *nosqlplugin.DomainRow,
) error {
	panic("TODO")
}

// Get one domain data, either by domainID or domainName
func (db *mdb) SelectDomain(
	ctx context.Context,
	domainID *string,
	domainName *string,
) (*nosqlplugin.DomainRow, error) {
	panic("TODO")
}

// Get all domain data
func (db *mdb) SelectAllDomains(
	ctx context.Context,
	pageSize int,
	pageToken []byte,
) ([]*nosqlplugin.DomainRow, []byte, error) {
	panic("TODO")
}

// Delete a domain, either by domainID or domainName
func (db *mdb) DeleteDomain(
	ctx context.Context,
	domainID *string,
	domainName *string,
) error {
	panic("TODO")
}

func (db *mdb) SelectDomainMetadata(
	ctx context.Context,
) (int64, error) {
	panic("TODO")
}
