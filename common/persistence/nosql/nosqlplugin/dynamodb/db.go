package dynamodb

import (
	"errors"
	"fmt"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

const (
	// PluginName is the name of the plugin
	PluginName = "dynamodb"
)

var (
	errConditionFailed = errors.New("internal condition fail error")
)

// ddb represents a logical connection to DynamoDB database
type ddb struct {
}

var _ nosqlplugin.DB = (*ddb)(nil)

// NewDynamoDB return a new DB
func NewDynamoDB(cfg config.NoSQL, logger log.Logger) (nosqlplugin.DB, error) {
	return nil, fmt.Errorf("TODO")
}

func (db *ddb) Close() {
	panic("TODO")
}

func (db *ddb) PluginName() string {
	return PluginName
}

func (db *ddb) IsNotFoundError(err error) bool {
	panic("TODO")
}

func (db *ddb) IsTimeoutError(err error) bool {
	panic("TODO")
}

func (db *ddb) IsThrottlingError(err error) bool {
	panic("TODO")
}

func (db *ddb) IsDBUnavailableError(err error) bool {
	panic("TODO")
}

func (db *ddb) IsConditionFailedError(err error) bool {
	return err == errConditionFailed
}
