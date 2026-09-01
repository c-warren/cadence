package mysql

const (
	_selectLatestConfigQuery = "SELECT row_type, version, timestamp, data, data_encoding FROM cluster_config WHERE row_type = ? ORDER BY version LIMIT 1;"

	_insertConfigQuery = "INSERT INTO cluster_config (row_type, version, timestamp, data, data_encoding) VALUES(?, ?, ?, ?, ?)"
)
