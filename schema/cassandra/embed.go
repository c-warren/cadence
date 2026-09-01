package cassandra

import "embed"

//go:embed cadence/* visibility/*
var SchemaFS embed.FS
