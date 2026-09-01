package public

import (
	"testing"

	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
)

// NewTestBaseWithPublicCassandra returns a persistence test base backed by cassandra datastore
// It is only being used by testing against external/public Cassandra, which require to load the default gocql client
func NewTestBaseWithPublicCassandra(t *testing.T, options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	if options.DBPluginName == "" {
		options.DBPluginName = "cassandra"
	}
	return persistencetests.NewTestBaseWithNoSQL(t, options)
}
