package nosql

import (
	"context"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

type nosqlDomainAuditStore struct {
	nosqlStore
}

// newNoSQLDomainAuditStore is used to create an instance of DomainAuditStore implementation
func newNoSQLDomainAuditStore(
	cfg config.ShardedNoSQL,
	logger log.Logger,
	metricsClient metrics.Client,
	dc *persistence.DynamicConfiguration,
) (persistence.DomainAuditStore, error) {
	shardedStore, err := newShardedNosqlStore(cfg, logger, metricsClient, dc, false)
	if err != nil {
		return nil, err
	}
	return &nosqlDomainAuditStore{
		nosqlStore: shardedStore.GetDefaultShard(),
	}, nil
}

// CreateDomainAuditLog creates a new domain audit log entry
func (m *nosqlDomainAuditStore) CreateDomainAuditLog(
	ctx context.Context,
	request *persistence.InternalCreateDomainAuditLogRequest,
) (*persistence.CreateDomainAuditLogResponse, error) {
	row := &nosqlplugin.DomainAuditLogRow{
		DomainID:            request.DomainID,
		EventID:             request.EventID,
		StateBefore:         getDataBlobBytes(request.StateBefore),
		StateBeforeEncoding: getDataBlobEncoding(request.StateBefore),
		StateAfter:          getDataBlobBytes(request.StateAfter),
		StateAfterEncoding:  getDataBlobEncoding(request.StateAfter),
		OperationType:       request.OperationType,
		CreatedTime:         request.CreatedTime,
		LastUpdatedTime:     request.LastUpdatedTime,
		Identity:            request.Identity,
		IdentityType:        request.IdentityType,
		Comment:             request.Comment,
		TTLSeconds:          request.TTLSeconds,
	}

	err := m.db.InsertDomainAuditLog(ctx, row)
	if err != nil {
		return nil, convertCommonErrors(m.db, "CreateDomainAuditLog", err)
	}

	return &persistence.CreateDomainAuditLogResponse{
		EventID: request.EventID,
	}, nil
}

// GetDomainAuditLogs retrieves domain audit logs
func (m *nosqlDomainAuditStore) GetDomainAuditLogs(
	ctx context.Context,
	request *persistence.GetDomainAuditLogsRequest,
) (*persistence.InternalGetDomainAuditLogsResponse, error) {
	filter := &nosqlplugin.DomainAuditLogFilter{
		DomainID:       request.DomainID,
		OperationType:  request.OperationType,
		MinCreatedTime: request.MinCreatedTime,
		MaxCreatedTime: request.MaxCreatedTime,
		PageSize:       request.PageSize,
		NextPageToken:  request.NextPageToken,
	}

	rows, nextPageToken, err := m.db.SelectDomainAuditLogs(ctx, filter)
	if err != nil {
		return nil, convertCommonErrors(m.db, "GetDomainAuditLogs", err)
	}

	var auditLogs []*persistence.InternalDomainAuditLog
	for _, row := range rows {
		auditLog := &persistence.InternalDomainAuditLog{
			EventID:         row.EventID,
			DomainID:        row.DomainID,
			OperationType:   row.OperationType,
			CreatedTime:     row.CreatedTime,
			LastUpdatedTime: row.LastUpdatedTime,
			Identity:        row.Identity,
			IdentityType:    row.IdentityType,
			Comment:         row.Comment,
		}

		if len(row.StateBefore) > 0 {
			auditLog.StateBefore = &persistence.DataBlob{
				Encoding: constants.EncodingType(row.StateBeforeEncoding),
				Data:     row.StateBefore,
			}
		}

		if len(row.StateAfter) > 0 {
			auditLog.StateAfter = &persistence.DataBlob{
				Encoding: constants.EncodingType(row.StateAfterEncoding),
				Data:     row.StateAfter,
			}
		}

		auditLogs = append(auditLogs, auditLog)
	}

	return &persistence.InternalGetDomainAuditLogsResponse{
		AuditLogs:     auditLogs,
		NextPageToken: nextPageToken,
	}, nil
}

func getDataBlobBytes(blob *persistence.DataBlob) []byte {
	if blob == nil {
		return nil
	}
	return blob.Data
}

func getDataBlobEncoding(blob *persistence.DataBlob) string {
	if blob == nil {
		return ""
	}
	return string(blob.Encoding)
}
