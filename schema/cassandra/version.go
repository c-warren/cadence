package cassandra

import "github.com/uber/cadence/schema/common"

// NOTE: whenever there is a new data base schema update, plz update the following versions

// Version is the Cassandra database release version
const Version = "0.51"

// VisibilityVersion is the Cassandra visibility database release version
const VisibilityVersion = "0.10"

var (
	DefaultSchema    = common.EmbeddedSchema(SchemaFS, Version, "cadence", "schema.cql")
	VisibilitySchema = common.EmbeddedSchema(SchemaFS, VisibilityVersion, "visibility", "schema.cql")
)
