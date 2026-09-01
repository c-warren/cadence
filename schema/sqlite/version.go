package sqlite

import (
	"github.com/uber/cadence/schema/common"
)

// NOTE: whenever there is a new data base schema update, plz update the following versions

// Version is the SQLite database release version
const Version = "0.3"

// VisibilityVersion is the SQLite visibility database release version
const VisibilityVersion = "0.2"

var (
	DefaultSchema    = common.EmbeddedSchema(SchemaFS, Version, "cadence", "schema.sql")
	VisibilitySchema = common.EmbeddedSchema(SchemaFS, VisibilityVersion, "visibility", "schema.sql")
)
