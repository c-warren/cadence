package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/persistence/serialization"
	"github.com/uber/cadence/common/persistence/sql/sqldriver"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

func TestInsertIntoActiveClusterSelectionPolicy(t *testing.T) {
	numDBShards := 30
	testHistoryShardID := 100
	testDomainID := serialization.MustParseUUID("10000000-0000-f000-f000-000000000001")
	testWorkflowID := "test-workflow-id"
	testRunID := serialization.MustParseUUID("30000000-0000-f000-f000-000000000001")
	testData := []byte("test-data-123")
	testDataEncoding := "thriftrw"
	driverErr := errors.New("driver error")

	tests := []struct {
		name      string
		row       *sqlplugin.ActiveClusterSelectionPolicyRow
		mockSetup func(*sqldriver.MockDriver)
		wantErr   error
	}{
		{
			name: "successfully inserted active_cluster_selection_policy",
			row: &sqlplugin.ActiveClusterSelectionPolicyRow{
				ShardID:      testHistoryShardID,
				DomainID:     testDomainID,
				WorkflowID:   testWorkflowID,
				RunID:        testRunID,
				Data:         testData,
				DataEncoding: testDataEncoding,
			},
			mockSetup: func(mockDriver *sqldriver.MockDriver) {
				mockDriver.EXPECT().ExecContext(
					gomock.Any(),
					testHistoryShardID%numDBShards,
					insertActiveClusterSelectionPolicyQry,
					testHistoryShardID,
					testDomainID,
					testWorkflowID,
					testRunID,
					testData,
					testDataEncoding,
				).Return(nil, nil)
			},
			wantErr: nil,
		},
		{
			name: "driver error occurred",
			row: &sqlplugin.ActiveClusterSelectionPolicyRow{
				ShardID:      testHistoryShardID,
				DomainID:     testDomainID,
				WorkflowID:   testWorkflowID,
				RunID:        testRunID,
				Data:         testData,
				DataEncoding: testDataEncoding,
			},
			mockSetup: func(mockDriver *sqldriver.MockDriver) {
				mockDriver.EXPECT().ExecContext(
					gomock.Any(),
					testHistoryShardID%numDBShards,
					insertActiveClusterSelectionPolicyQry,
					testHistoryShardID,
					testDomainID,
					testWorkflowID,
					testRunID,
					testData,
					testDataEncoding,
				).Return(nil, driverErr)
			},
			wantErr: driverErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockDriver := sqldriver.NewMockDriver(ctrl)
			tc.mockSetup(mockDriver)

			pdb := &db{
				driver:      mockDriver,
				converter:   &converter{},
				numDBShards: numDBShards,
			}

			_, err := pdb.InsertIntoActiveClusterSelectionPolicy(context.Background(), tc.row)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSelectFromActiveClusterSelectionPolicy(t *testing.T) {
	numDBShards := 50
	testHistoryShardID := 999
	testDomainID := serialization.MustParseUUID("10000000-0000-f000-f000-000000000001")
	testWorkflowID := "test-workflow-id"
	testRunID := serialization.MustParseUUID("30000000-0000-f000-f000-000000000001")
	tests := []struct {
		name      string
		filter    *sqlplugin.ActiveClusterSelectionPolicyFilter
		mockSetup func(*sqldriver.MockDriver)
		wantErr   error
	}{
		{
			name: "successfully selected one active_cluster_selection_policy row",
			filter: &sqlplugin.ActiveClusterSelectionPolicyFilter{
				ShardID:    testHistoryShardID,
				DomainID:   testDomainID,
				WorkflowID: testWorkflowID,
				RunID:      testRunID,
			},
			mockSetup: func(mockDriver *sqldriver.MockDriver) {
				mockDriver.EXPECT().GetContext(
					gomock.Any(),
					testHistoryShardID%numDBShards,
					gomock.Any(),
					selectActiveClusterSelectionPolicyQry,
					testHistoryShardID,
					testDomainID,
					testWorkflowID,
					testRunID,
				).DoAndReturn(func(_ context.Context, _ int, dest interface{}, _ string, _ ...interface{}) error {
					row := dest.(*sqlplugin.ActiveClusterSelectionPolicyRow)
					row.ShardID = testHistoryShardID
					row.DomainID = testDomainID
					row.WorkflowID = testWorkflowID
					row.RunID = testRunID
					row.Data = []byte("test-data-123")
					row.DataEncoding = "thriftrw"
					return nil
				})
			},
			wantErr: nil,
		},
		{
			name: "no matched row",
			filter: &sqlplugin.ActiveClusterSelectionPolicyFilter{
				ShardID:    testHistoryShardID,
				DomainID:   testDomainID,
				WorkflowID: testWorkflowID,
				RunID:      testRunID,
			},
			mockSetup: func(mockDriver *sqldriver.MockDriver) {
				mockDriver.EXPECT().GetContext(
					gomock.Any(),
					testHistoryShardID%numDBShards,
					gomock.Any(),
					selectActiveClusterSelectionPolicyQry,
					testHistoryShardID,
					testDomainID,
					testWorkflowID,
					testRunID,
				).Return(sql.ErrNoRows)
			},
			wantErr: sql.ErrNoRows,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockDriver := sqldriver.NewMockDriver(ctrl)
			tc.mockSetup(mockDriver)

			pdb := &db{
				driver:      mockDriver,
				converter:   &converter{},
				numDBShards: numDBShards,
			}

			result, err := pdb.SelectFromActiveClusterSelectionPolicy(context.Background(), tc.filter)
			if tc.wantErr != nil {
				assert.Nil(t, result)
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, testHistoryShardID, result.ShardID)
				assert.Equal(t, testDomainID, result.DomainID)
				assert.Equal(t, testWorkflowID, result.WorkflowID)
				assert.Equal(t, testRunID, result.RunID)
				assert.Equal(t, []byte("test-data-123"), result.Data)
				assert.Equal(t, "thriftrw", result.DataEncoding)
			}
		})
	}
}

func TestDeleteFromActiveClusterSelectionPolicy(t *testing.T) {
	numDBShards := 800
	testHistoryShardID := 1111
	testDomainID := serialization.MustParseUUID("10000000-0000-f000-f000-000000000001")
	testWorkflowID := "test-workflow-id"
	testRunID := serialization.MustParseUUID("30000000-0000-f000-f000-000000000001")
	contextTimeoutErr := context.DeadlineExceeded
	tests := []struct {
		name      string
		filter    *sqlplugin.ActiveClusterSelectionPolicyFilter
		mockSetup func(*sqldriver.MockDriver)
		wantErr   error
	}{
		{
			name: "successfully deleted one active_cluster_selection_policy row",
			filter: &sqlplugin.ActiveClusterSelectionPolicyFilter{
				ShardID:    testHistoryShardID,
				DomainID:   testDomainID,
				WorkflowID: testWorkflowID,
				RunID:      testRunID,
			},
			mockSetup: func(mockDriver *sqldriver.MockDriver) {
				mockDriver.EXPECT().ExecContext(
					gomock.Any(),
					testHistoryShardID%numDBShards,
					deleteActiveClusterSelectionPolicyQry,
					testHistoryShardID,
					testDomainID,
					testWorkflowID,
					testRunID,
				).Return(nil, nil)
			},
			wantErr: nil,
		},
		{
			name: "context timeout error occurred",
			filter: &sqlplugin.ActiveClusterSelectionPolicyFilter{
				ShardID:    testHistoryShardID,
				DomainID:   testDomainID,
				WorkflowID: testWorkflowID,
				RunID:      testRunID,
			},
			mockSetup: func(mockDriver *sqldriver.MockDriver) {
				mockDriver.EXPECT().ExecContext(
					gomock.Any(),
					testHistoryShardID%numDBShards,
					deleteActiveClusterSelectionPolicyQry,
					testHistoryShardID,
					testDomainID,
					testWorkflowID,
					testRunID,
				).Return(nil, contextTimeoutErr)
			},
			wantErr: contextTimeoutErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockDriver := sqldriver.NewMockDriver(ctrl)
			tc.mockSetup(mockDriver)

			pdb := &db{
				driver:      mockDriver,
				converter:   &converter{},
				numDBShards: numDBShards,
			}

			// Delete inserted row
			_, err := pdb.DeleteFromActiveClusterSelectionPolicy(context.Background(), tc.filter)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
