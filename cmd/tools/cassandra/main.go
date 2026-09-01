package main

import (
	"os"

	"github.com/uber/cadence/tools/cassandra"
	"github.com/uber/cadence/tools/common/commoncli"

	_ "github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql/public" // needed to load the default gocql client
)

func main() {
	app := cassandra.BuildCLIOptions()
	commoncli.ExitHandler(app.Run(os.Args))
}
