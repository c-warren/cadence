package main

import (
	"os"

	"github.com/uber/cadence/tools/common/commoncli"
	"github.com/uber/cadence/tools/sql"

	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"    // needed to load mysql plugin
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/postgres" // needed to load postgres plugin
)

func main() {
	commoncli.ExitHandler(sql.RunTool(os.Args))
}
