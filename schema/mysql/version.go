package mysql

import "github.com/uber/cadence/schema/common"

// NOTE: whenever there is a new data base schema update, plz update the following versions

// Version is the MySQL database release version
const Version = "0.8"

// VisibilityVersion is the MySQL visibility database release version
const VisibilityVersion = "0.8"

var (
	DefaultSchema    = common.EmbeddedSchema(SchemaFS, Version, "v8/cadence", "schema.sql")
	VisibilitySchema = common.EmbeddedSchema(SchemaFS, VisibilityVersion, "v8/visibility", "schema.sql")
)
