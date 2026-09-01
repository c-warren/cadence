package mongodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *mdb) InsertVisibility(
	ctx context.Context,
	ttlSeconds int64,
	row *nosqlplugin.VisibilityRowForInsert,
) error {
	panic("TODO")
}

func (db *mdb) UpdateVisibility(
	ctx context.Context,
	ttlSeconds int64,
	row *nosqlplugin.VisibilityRowForUpdate,
) error {
	panic("TODO")
}

func (db *mdb) SelectVisibility(
	ctx context.Context,
	filter *nosqlplugin.VisibilityFilter,
) (*nosqlplugin.SelectVisibilityResponse, error) {
	panic("TODO")
}

func (db *mdb) DeleteVisibility(
	ctx context.Context,
	domainID, workflowID, runID string,
) error {
	panic("TODO")
}

func (db *mdb) SelectOneClosedWorkflow(
	ctx context.Context,
	domainID, workflowID, runID string,
) (*nosqlplugin.VisibilityRow, error) {
	panic("TODO")
}
