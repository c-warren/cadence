package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/schema/mongodb/cadence"
)

func (db *mdb) InsertConfig(ctx context.Context, row *persistence.InternalConfigStoreEntry) error {
	collection := db.dbConn.Collection(cadence.ClusterConfigCollectionName)
	doc := cadence.ClusterConfigCollectionEntry{
		RowType:              row.RowType,
		Version:              row.Version,
		UnixTimestampSeconds: row.Timestamp.Unix(),
		Data:                 row.Values.Data,
		DataEncoding:         row.Values.GetEncodingString(),
	}
	_, err := collection.InsertOne(ctx, doc)
	if mongo.IsDuplicateKeyError(err) {
		return nosqlplugin.NewConditionFailure("InsertConfig operation failed because of version collision")
	}
	return err
}

func (db *mdb) SelectLatestConfig(ctx context.Context, rowType int) (*persistence.InternalConfigStoreEntry, error) {
	filter := bson.D{{"rowtype", rowType}}
	queryOptions := options.FindOneOptions{}
	queryOptions.SetSort(bson.D{{"version", -1}})

	collection := db.dbConn.Collection(cadence.ClusterConfigCollectionName)
	var result cadence.ClusterConfigCollectionEntry
	err := collection.FindOne(ctx, filter, &queryOptions).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalConfigStoreEntry{
		RowType:   rowType,
		Version:   result.Version,
		Timestamp: time.Unix(result.UnixTimestampSeconds, 0),
		Values:    persistence.NewDataBlob(result.Data, constants.EncodingType(result.DataEncoding)),
	}, nil
}
