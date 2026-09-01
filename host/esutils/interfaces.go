package esutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type (
	// ESClient is ElasicSearch client for running test suite to be implemented in different versions of ES.
	// Those interfaces are only being used by tests so we don't implement in common/elasticsearch pkg.
	ESClient interface {
		PutIndexTemplate(t *testing.T, templateConfigFile, templateName string)
		CreateIndex(t *testing.T, indexName string)
		DeleteIndex(t *testing.T, indexName string)
		PutMaxResultWindow(t *testing.T, indexName string, maxResultWindow int) error
		GetMaxResultWindow(t *testing.T, indexName string) (string, error)
	}
)

// CreateESClient create ElasticSearch client for test
func CreateESClient(t *testing.T, url string, version string) ESClient {
	var client ESClient
	var err error
	switch version {
	case "v6":
		client, err = newV6Client(url)
	case "v7":
		client, err = newV7Client(url)
	case "os2":
		client, err = newOS2Client(url)
	default:
		assert.FailNow(t, fmt.Sprintf("not supported ES version: %s", version))
	}
	assert.NoError(t, err)
	return client
}

func createContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	return ctx, cancel
}
