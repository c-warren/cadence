package postgres

import "github.com/uber/cadence/schema/common"

// NOTE: whenever there is a new data base schema update, plz update the following versions

// Version is the Postgres database release version
// Cadence supports both MySQL and Postgres officially, so upgrade should be perform for both MySQL and Postgres
const Version = "0.8"

// VisibilityVersion is the Postgres visibility database release version
// Cadence supports both MySQL and Postgres officially, so upgrade should be perform for both MySQL and Postgres
const VisibilityVersion = "0.9"

var (
	DefaultSchema    = common.EmbeddedSchema(SchemaFS, Version, "cadence", "schema.sql")
	VisibilitySchema = common.EmbeddedSchema(SchemaFS, VisibilityVersion, "visibility", "schema.sql")
)
