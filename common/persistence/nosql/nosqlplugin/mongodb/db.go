package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// mdb represents a logical connection to MongoDB database
type mdb struct {
	client *mongo.Client
	dbConn *mongo.Database
	cfg    *config.NoSQL
	logger log.Logger
}

var _ nosqlplugin.DB = (*mdb)(nil)

func (db *mdb) Close() {
	db.client.Disconnect(context.Background())
}

func (db *mdb) PluginName() string {
	return PluginName
}
