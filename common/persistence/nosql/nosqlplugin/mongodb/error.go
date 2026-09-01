package mongodb

import "go.mongodb.org/mongo-driver/mongo"

func (db *mdb) IsNotFoundError(err error) bool {
	return err == mongo.ErrNoDocuments
}

func (db *mdb) IsTimeoutError(err error) bool {
	return mongo.IsTimeout(err) || mongo.IsNetworkError(err)
}

func (db *mdb) IsThrottlingError(err error) bool {
	return false
}

func (db *mdb) IsDBUnavailableError(err error) bool {
	return false
}
