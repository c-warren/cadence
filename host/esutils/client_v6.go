package esutils

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/stretchr/testify/require"
)

type (
	v6Client struct {
		client *elastic.Client
	}
)

func newV6Client(url string) (*v6Client, error) {
	esClient, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),
	)
	return &v6Client{
		client: esClient,
	}, err
}

func (es *v6Client) PutIndexTemplate(t *testing.T, templateConfigFile, templateName string) {
	// This function is used exclusively in tests. Excluding it from security checks.
	// #nosec
	template, err := os.ReadFile(templateConfigFile)
	require.NoError(t, err)
	ctx, cancel := createContext()
	defer cancel()
	putTemplate, err := es.client.IndexPutTemplate(templateName).BodyString(string(template)).Do(ctx)
	require.NoError(t, err)
	require.True(t, putTemplate.Acknowledged)
}

func (es *v6Client) CreateIndex(t *testing.T, indexName string) {
	ctx, cancel := createContext()
	defer cancel()
	exists, err := es.client.IndexExists(indexName).Do(ctx)
	require.NoError(t, err)
	if exists {
		ctx, cancel := createContext()
		defer cancel()
		deleteTestIndex, err := es.client.DeleteIndex(indexName).Do(ctx)
		require.Nil(t, err)
		require.True(t, deleteTestIndex.Acknowledged)
	}

	ctx, cancel = createContext()
	defer cancel()
	createTestIndex, err := es.client.CreateIndex(indexName).Do(ctx)
	require.Nil(t, err)
	require.True(t, createTestIndex.Acknowledged)
}

func (es *v6Client) DeleteIndex(t *testing.T, indexName string) {
	ctx, cancel := createContext()
	defer cancel()
	deleteTestIndex, err := es.client.DeleteIndex(indexName).Do(ctx)
	require.Nil(t, err)
	require.True(t, deleteTestIndex.Acknowledged)
}

func (es *v6Client) PutMaxResultWindow(t *testing.T, indexName string, maxResultWindow int) error {
	ctx, cancel := createContext()
	defer cancel()
	_, err := es.client.IndexPutSettings(indexName).
		BodyString(fmt.Sprintf(`{"max_result_window" : %d}`, maxResultWindow)).
		Do(ctx)
	require.NoError(t, err)
	return err
}

func (es *v6Client) GetMaxResultWindow(t *testing.T, indexName string) (string, error) {
	ctx, cancel := createContext()
	defer cancel()
	settings, err := es.client.IndexGetSettings(indexName).Do(ctx)
	require.NoError(t, err)
	return settings[indexName].Settings["index"].(map[string]interface{})["max_result_window"].(string), nil
}
