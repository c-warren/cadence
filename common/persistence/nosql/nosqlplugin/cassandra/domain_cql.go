package cassandra

const (
	templateDomainInfoType = `{` +
		`id: ?, ` +
		`name: ?, ` +
		`status: ?, ` +
		`description: ?, ` +
		`owner_email: ?, ` +
		`data: ? ` +
		`}`

	templateDomainConfigType = `{` +
		`retention: ?, ` +
		`emit_metric: ?, ` +
		`archival_bucket: ?, ` +
		`archival_status: ?,` +
		`history_archival_status: ?, ` +
		`history_archival_uri: ?, ` +
		`visibility_archival_status: ?, ` +
		`visibility_archival_uri: ?, ` +
		`bad_binaries: ?,` +
		`bad_binaries_encoding: ?,` +
		`isolation_groups: ?,` +
		`isolation_groups_encoding: ?,` +
		`async_workflow_config: ?,` +
		`async_workflow_config_encoding: ?` +
		`}`

	templateDomainReplicationConfigType = `{` +
		`active_cluster_name: ?, ` +
		`clusters: ?, ` +
		`active_clusters_config: ?, ` +
		`active_clusters_config_encoding: ?` +
		`}`

	templateCreateDomainQuery = `INSERT INTO domains (` +
		`id, domain, created_time) ` +
		`VALUES(?, {name: ?}, ?) IF NOT EXISTS`

	templateGetDomainQuery = `SELECT domain.name ` +
		`FROM domains ` +
		`WHERE id = ?`

	templateDeleteDomainQuery = `DELETE FROM domains ` +
		`WHERE id = ?`

	templateCreateDomainByNameQueryWithinBatchV2 = `INSERT INTO domains_by_name_v2 (` +
		`domains_partition, name, domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, previous_failover_version, failover_end_time, last_updated_time, notification_version, created_time) ` +
		`VALUES(?, ?, ` + templateDomainInfoType + `, ` + templateDomainConfigType + `, ` + templateDomainReplicationConfigType + `, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetDomainByNameQueryV2 = `SELECT domain.id, domain.name, domain.status, domain.description, ` +
		`domain.owner_email, domain.data, config.retention, config.emit_metric, ` +
		`config.archival_bucket, config.archival_status, ` +
		`config.history_archival_status, config.history_archival_uri, ` +
		`config.visibility_archival_status, config.visibility_archival_uri, ` +
		`config.bad_binaries, config.bad_binaries_encoding, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`replication_config.active_clusters_config, replication_config.active_clusters_config_encoding, ` +
		`config.isolation_groups,` +
		`config.isolation_groups_encoding,` +
		`config.async_workflow_config,` +
		`config.async_workflow_config_encoding,` +
		`is_global_domain, ` +
		`config_version, ` +
		`failover_version, ` +
		`failover_notification_version, ` +
		`previous_failover_version, ` +
		`failover_end_time, ` +
		`last_updated_time, ` +
		`notification_version ` +
		`FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? ` +
		`and name = ?`

	templateUpdateDomainByNameQueryWithinBatchV2 = `UPDATE domains_by_name_v2 ` +
		`SET domain = ` + templateDomainInfoType + `, ` +
		`config = ` + templateDomainConfigType + `, ` +
		`replication_config = ` + templateDomainReplicationConfigType + `, ` +
		`config_version = ? ,` +
		`failover_version = ? ,` +
		`failover_notification_version = ? , ` +
		`previous_failover_version = ? , ` +
		`failover_end_time = ?,` +
		`last_updated_time = ?,` +
		`notification_version = ? ` +
		`WHERE domains_partition = ? ` +
		`and name = ?`

	templateGetMetadataQueryV2 = `SELECT notification_version ` +
		`FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? ` +
		`and name = ? `

	templateUpdateMetadataQueryWithinBatchV2 = `UPDATE domains_by_name_v2 ` +
		`SET notification_version = ? ` +
		`WHERE domains_partition = ? ` +
		`and name = ? ` +
		`IF notification_version = ? `

	templateDeleteDomainByNameQueryV2 = `DELETE FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? ` +
		`and name = ?`

	templateListDomainQueryV2 = `SELECT name, domain.id, domain.name, domain.status, domain.description, ` +
		`domain.owner_email, domain.data, config.retention, config.emit_metric, ` +
		`config.archival_bucket, config.archival_status, ` +
		`config.history_archival_status, config.history_archival_uri, ` +
		`config.visibility_archival_status, config.visibility_archival_uri, ` +
		`config.bad_binaries, config.bad_binaries_encoding, ` +
		`config.isolation_groups, config.isolation_groups_encoding, ` +
		`config.async_workflow_config, config.async_workflow_config_encoding, ` +
		`replication_config.active_cluster_name, replication_config.clusters, ` +
		`replication_config.active_clusters_config, replication_config.active_clusters_config_encoding, ` +
		`is_global_domain, ` +
		`config_version, ` +
		`failover_version, ` +
		`failover_notification_version, ` +
		`previous_failover_version, ` +
		`failover_end_time, ` +
		`last_updated_time, ` +
		`notification_version ` +
		`FROM domains_by_name_v2 ` +
		`WHERE domains_partition = ? `
)
