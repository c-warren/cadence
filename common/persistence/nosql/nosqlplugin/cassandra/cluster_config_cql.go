package cassandra

const (
	// version is the clustering key(DESC order) so this query will always return the record with largest version
	templateSelectLatestConfig = `SELECT row_type, version, timestamp, values, encoding FROM cluster_config ` +
		`WHERE row_type = ? ` +
		`LIMIT 1;`

	templateInsertConfig = `INSERT INTO cluster_config (row_type, version, timestamp, values, encoding) ` +
		`VALUES (?, ?, ?, ?, ?) ` +
		`IF NOT EXISTS;`
)
