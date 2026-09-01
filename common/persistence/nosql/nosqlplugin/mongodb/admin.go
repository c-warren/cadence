package mongodb

import (
	"context"
	"io/ioutil"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

const (
	testSchemaDir = "schema/mongodb/"
)

func (db *mdb) SetupTestDatabase(schemaBaseDir string, replicas int) error {
	if schemaBaseDir == "" {
		var err error
		schemaBaseDir, err = nosqlplugin.GetDefaultTestSchemaDir(testSchemaDir)
		if err != nil {
			return err
		}
	}

	schemaFile := schemaBaseDir + "cadence/schema.json"
	byteValues, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	var commands []interface{}
	err = bson.UnmarshalExtJSON(byteValues, false, &commands)
	if err != nil {
		return err
	}
	for _, cmd := range commands {
		result := db.dbConn.RunCommand(context.Background(), cmd)
		if result.Err() != nil {
			return result.Err()
		}
	}
	return nil
}

func (db *mdb) TeardownTestDatabase() error {
	result := db.dbConn.RunCommand(context.Background(), bson.D{{"dropDatabase", 1}})
	err := result.Err()
	return err
}
