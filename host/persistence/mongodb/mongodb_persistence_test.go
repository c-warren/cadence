package tests

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin/mongodb"
	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/testflags"
)

func TestMongoDBConfigStorePersistence(t *testing.T) {
	testflags.RequireMongoDB(t)
	s := new(persistencetests.ConfigStorePersistenceSuite)
	s.TestBase = NewTestBaseWithMongo(t)
	s.TestBase.Setup()
	suite.Run(t, s)
}

// TODO uncomment the test once HistoryEventsCRUD is implemented
// func TestMongoDBHistoryPersistence(t *testing.T) {
// 	s := new(persistencetests.HistoryV2PersistenceSuite)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once TaskCRUD is implemented
// func TestMongoDBMatchingPersistence(t *testing.T) {
// 	s := new(persistencetests.MatchingPersistenceSuite)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once DomainCRUD is implemented
// func TestMongoDBDomainPersistence(t *testing.T) {
// 	s := new(persistencetests.MetadataPersistenceSuiteV2)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once MessageQueueCRUD is implemented
// func TestMongoDBQueuePersistence(t *testing.T) {
// 	s := new(persistencetests.QueuePersistenceSuite)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once ShardCRUD is implemented
// func TestMongoDBShardPersistence(t *testing.T) {
// 	s := new(persistencetests.ShardPersistenceSuite)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once VisibilityCRUD is implemented
// func TestMongoDBVisibilityPersistence(t *testing.T) {
// 	s := new(persistencetests.DBVisibilityPersistenceSuite)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once WorkflowCRUD is implemented
// func TestMongoDBExecutionManager(t *testing.T) {
// 	s := new(persistencetests.ExecutionManagerSuite)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

// TODO uncomment the test once WorkflowCRUD is implemented
// func TestMongoDBExecutionManagerWithEventsV2(t *testing.T) {
// 	s := new(persistencetests.ExecutionManagerSuiteForEventsV2)
// 	s.TestBase = NewTestBaseWithMongo()
// 	s.TestBase.Setup()
// 	suite.Run(t, s)
// }

func NewTestBaseWithMongo(t *testing.T) *persistencetests.TestBase {
	port, err := environment.GetMongoPort()
	if err != nil {
		t.Fatal(err)
	}

	options := &persistencetests.TestBaseOptions{
		DBPluginName: mongodb.PluginName,
		DBHost:       environment.GetMongoAddress(),
		DBUsername:   "root",
		DBPassword:   "cadence",
		DBPort:       port,
	}
	return persistencetests.NewTestBaseWithNoSQL(t, options)
}
