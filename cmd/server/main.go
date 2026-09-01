package main

import (
	"os"

	"github.com/uber/cadence/cmd/server/cadence"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/tools/common/commoncli"

	_ "github.com/ncruces/go-sqlite3/driver"                                                // register sqlite3 driver
	_ "github.com/ncruces/go-sqlite3/embed"                                                 // embed sqlite db
	_ "github.com/uber/cadence/common/archiver/gcloud"                                      // needed to load the optional gcloud archiver plugin
	_ "github.com/uber/cadence/common/asyncworkflow/queue/kafka"                            // needed to load kafka asyncworkflow queue
	_ "github.com/uber/cadence/common/dynamicconfig/openfeatureprovider/unleash"            // needed to load the optional unleash openfeature provider plugin
	_ "github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra"              // needed to load cassandra plugin
	_ "github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql/public" // needed to load the default gocql client
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/cloudsql-mysql"             // needed to load cloudsql-mysql plugin
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"                      // needed to load mysql plugin
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/postgres"                   // needed to load postgres plugin
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/sqlite"                     // needed to load sqlite plugin
	_ "net/http/pprof"                                                                      // register pprof HTTP handlers
)

// main entry point for the cadence server
func main() {
	app := cadence.BuildCLI(metrics.ReleaseVersion, metrics.Revision)
	commoncli.ExitHandler(app.Run(os.Args))
}
