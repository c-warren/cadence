//go:build tools
// +build tools

package tools

import (
	// forces one import-order pattern
	_ "github.com/daixiang0/gci"
	// enumer for generating utility methods for const enums
	_ "github.com/dmarkham/enumer"
	// protobuf stuff
	_ "github.com/gogo/protobuf/protoc-gen-gofast"
	// gowrap for generating decorators for interface
	_ "github.com/hexdigest/gowrap"
	// replaces golint - configurable and much faster
	_ "github.com/mgechev/revive"
	// mockery for generating mocks
	_ "github.com/vektra/mockery/v3"
	// mockgen for generating mocks
	_ "go.uber.org/mock/mockgen"
	// nilaway for nil pointer analysis
	_ "go.uber.org/nilaway"
	// thriftrw code gen
	_ "go.uber.org/thriftrw"
	_ "go.uber.org/yarpc/encoding/protobuf/protoc-gen-yarpc-go"
	// yarpc plugin for thriftrw code gen
	_ "go.uber.org/yarpc/encoding/thrift/thriftrw-plugin-yarpc"
	// removes unused imports and formats
	_ "golang.org/x/tools/cmd/goimports"
)
