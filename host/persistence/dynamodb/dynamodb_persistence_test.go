package dynamodb

import (
	"testing"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin/dynamodb"
)

// This is to make sure adding new noop method when adding new nosql interfaces
// Remove it when any other tests are implemented.
func TestDynamoDBNoopStruct(t *testing.T) {
	_, _ = dynamodb.NewDynamoDB(config.NoSQL{}, nil)
}

func TestDynamoDBHistoryPersistence(t *testing.T) {
	// s := new(persistencetests.HistoryV2PersistenceSuite)
	// s.TestBase = public.NewTestBaseWithDynamoDB(&persistencetests.TestBaseOptions{})
	// s.TestBase.Setup()
	// suite.Run(t, s)
}

func TestDynamoDBMatchingPersistence(t *testing.T) {
	// s := new(persistencetests.MatchingPersistenceSuite)
	// s.TestBase = public.NewTestBaseWithDynamoDB(&persistencetests.TestBaseOptions{})
	// s.TestBase.Setup()
	// suite.Run(t, s)
}

func TestDynamoDBDomainPersistence(t *testing.T) {
	// s := new(persistencetests.MetadataPersistenceSuiteV2)
	// s.TestBase = public.NewTestBaseWithDynamoDB(&persistencetests.TestBaseOptions{})
	// s.TestBase.Setup()
	// suite.Run(t, s)
}

func TestDynamoDBQueuePersistence(t *testing.T) {
	// s := new(persistencetests.QueuePersistenceSuite)
	// s.TestBase = public.NewTestBaseWithDynamoDB(&persistencetests.TestBaseOptions{})
	// s.TestBase.Setup()
	// suite.Run(t, s)
}
