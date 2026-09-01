package mysql

import "embed"

//go:embed v8/cadence/* v8/visibility/*
var SchemaFS embed.FS
