package main

import (
	"os"

	"go.uber.org/zap"

	"github.com/uber/cadence/tools/cli"
	"github.com/uber/cadence/tools/common/commoncli"

	_ "github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra"              // needed to load cassandra plugin
	_ "github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql/public" // needed to load the default gocql client
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"                      // needed to load mysql plugin
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/postgres"                   // needed to load postgres plugin
)

// Start using this CLI tool with command
// See cadence/tools/cli/README.md for usage
func main() {
	app := cli.NewCliApp(cli.NewClientFactory(must(zap.NewDevelopment())))
	commoncli.ExitHandler(app.Run(os.Args))
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
