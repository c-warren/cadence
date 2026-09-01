package mongodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// InsertIntoHistoryTreeAndNode inserts one or two rows: tree row and node row(at least one of them)
func (db *mdb) InsertIntoHistoryTreeAndNode(ctx context.Context, treeRow *nosqlplugin.HistoryTreeRow, nodeRow *nosqlplugin.HistoryNodeRow) error {
	panic("TODO")
}

// SelectFromHistoryNode read nodes based on a filter
func (db *mdb) SelectFromHistoryNode(ctx context.Context, filter *nosqlplugin.HistoryNodeFilter) ([]*nosqlplugin.HistoryNodeRow, []byte, error) {
	panic("TODO")
}

// DeleteFromHistoryTreeAndNode delete a branch record, and a list of ranges of nodes.
func (db *mdb) DeleteFromHistoryTreeAndNode(ctx context.Context, treeFilter *nosqlplugin.HistoryTreeFilter, nodeFilters []*nosqlplugin.HistoryNodeFilter) error {
	panic("TODO")
}

// SelectAllHistoryTrees will return all tree branches with pagination
func (db *mdb) SelectAllHistoryTrees(ctx context.Context, nextPageToken []byte, pageSize int) ([]*nosqlplugin.HistoryTreeRow, []byte, error) {
	panic("TODO")
}

// SelectFromHistoryTree read branch records for a tree
func (db *mdb) SelectFromHistoryTree(ctx context.Context, filter *nosqlplugin.HistoryTreeFilter) ([]*nosqlplugin.HistoryTreeRow, error) {
	panic("TODO")
}
