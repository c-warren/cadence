package cassandra

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

const (
	templateInsertSemaphoreMetadataQuery = `INSERT INTO semaphore_metadata (` +
		`domain_id, semaphore_name, size, bucket_size, created_time) ` +
		`VALUES(?, ?, ?, ?, ?) IF NOT EXISTS`

	templateSelectSemaphoreMetadataQuery = `SELECT ` +
		`domain_id, semaphore_name, size, bucket_size, created_time ` +
		`FROM semaphore_metadata ` +
		`WHERE domain_id = ? AND semaphore_name = ?`

	templateSelectSemaphoreMetadataByDomainQuery = `SELECT ` +
		`domain_id, semaphore_name, size, bucket_size, created_time ` +
		`FROM semaphore_metadata ` +
		`WHERE domain_id = ?`
)

// InsertSemaphoreMetadata creates a semaphore's metadata with a conflict-detecting
// INSERT ... IF NOT EXISTS (LWT). It does not overwrite: if a row with the same
// (domain_id, semaphore_name) already exists, it returns a ConditionFailure.
func (db *CDB) InsertSemaphoreMetadata(ctx context.Context, row *nosqlplugin.SemaphoreMetadataRow) error {
	query := db.session.Query(templateInsertSemaphoreMetadataQuery,
		row.DomainID,
		row.SemaphoreName,
		row.Size,
		row.BucketSize,
		row.CreatedTime,
	).WithContext(ctx)

	applied, err := query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return err
	}
	if !applied {
		return nosqlplugin.NewConditionFailure("InsertSemaphoreMetadata operation failed because the semaphore already exists")
	}
	return nil
}

// SelectSemaphoreMetadata returns a single semaphore's metadata by (domainID, semaphoreName).
func (db *CDB) SelectSemaphoreMetadata(ctx context.Context, domainID, semaphoreName string) (*nosqlplugin.SemaphoreMetadataRow, error) {
	row := &nosqlplugin.SemaphoreMetadataRow{}
	query := db.session.Query(templateSelectSemaphoreMetadataQuery, domainID, semaphoreName).WithContext(ctx)
	err := query.Scan(
		&row.DomainID,
		&row.SemaphoreName,
		&row.Size,
		&row.BucketSize,
		&row.CreatedTime,
	)
	if err != nil {
		return nil, err
	}
	return row, nil
}

// SelectSemaphoreMetadataByDomain returns the semaphores in a domain, paginated.
func (db *CDB) SelectSemaphoreMetadataByDomain(ctx context.Context, filter *nosqlplugin.SemaphoreMetadataFilter) ([]*nosqlplugin.SemaphoreMetadataRow, []byte, error) {
	query := db.session.Query(templateSelectSemaphoreMetadataByDomainQuery, filter.DomainID).WithContext(ctx)

	if filter.PageSize > 0 {
		query = query.PageSize(filter.PageSize)
	}
	if len(filter.NextPageToken) > 0 {
		query = query.PageState(filter.NextPageToken)
	}

	iter := query.Iter()
	if iter == nil {
		return nil, nil, &types.InternalServiceError{
			Message: "SelectSemaphoreMetadataByDomain operation failed. Not able to create query iterator.",
		}
	}

	var rows []*nosqlplugin.SemaphoreMetadataRow
	row := &nosqlplugin.SemaphoreMetadataRow{}
	for iter.Scan(
		&row.DomainID,
		&row.SemaphoreName,
		&row.Size,
		&row.BucketSize,
		&row.CreatedTime,
	) {
		rows = append(rows, row)
		row = &nosqlplugin.SemaphoreMetadataRow{}

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
