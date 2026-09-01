package query

import (
	"encoding/json"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	qb := NewBuilder()
	qb.Query(NewExistsQuery("user"))
	qb.Size(10)
	qb.From(100)
	qb.Sortby(NewFieldSort("StartDate"))
	src, err := qb.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"from":100,"query":{"exists":{"field":"user"}},"size":10,"sort":[{"StartDate":{"order":"asc"}}]}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestBuilderAgainsESv7(t *testing.T) {
	qb := NewBuilder()
	qb.Query(NewExistsQuery("user"))
	qb.Size(10)
	qb.Sortby(NewFieldSort("runid").Desc())
	qb.Query(NewBoolQuery().Must(NewMatchQuery("domainID", "uuid"))).SearchAfter("sortval", "tiebraker")
	qbs, err := qb.Source()
	assert.NoError(t, err)

	searchSource := elastic.NewSearchSource().
		Query(elastic.NewExistsQuery("user")).
		Size(10).
		SortBy(elastic.NewFieldSort("runid").Desc()).
		Query(elastic.NewBoolQuery().Must(elastic.NewMatchQuery("domainID", "uuid"))).SearchAfter("sortval", "tiebraker")

	sss, err := searchSource.Source()
	assert.NoError(t, err)

	assert.Equal(t, sss, qbs, "ESv7 and local QueryBuilder should produce the same query")
}
