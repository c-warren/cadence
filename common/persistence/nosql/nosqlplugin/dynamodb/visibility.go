package dynamodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *ddb) InsertVisibility(
	ctx context.Context,
	ttlSeconds int64,
	row *nosqlplugin.VisibilityRowForInsert,
) error {
	panic("TODO")
}

func (db *ddb) UpdateVisibility(
	ctx context.Context,
	ttlSeconds int64,
	row *nosqlplugin.VisibilityRowForUpdate,
) error {
	panic("TODO")
}

func (db *ddb) SelectVisibility(
	ctx context.Context,
	filter *nosqlplugin.VisibilityFilter,
) (*nosqlplugin.SelectVisibilityResponse, error) {
	panic("TODO")
}

func (db *ddb) DeleteVisibility(
	ctx context.Context,
	domainID, workflowID, runID string,
) error {
	panic("TODO")
}

func (db *ddb) SelectOneClosedWorkflow(
	ctx context.Context,
	domainID, workflowID, runID string,
) (*nosqlplugin.VisibilityRow, error) {
	panic("TODO")
}
