package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
)

func BenchmarkGetIntProperty(b *testing.B) {
	client := NewInMemoryClient()
	cln := NewCollection(client, log.NewNoop())
	key := dynamicproperties.MatchingMaxTaskBatchSize
	for i := 0; i < b.N; i++ {
		size := cln.GetIntProperty(key)
		assert.Equal(b, 100, size())
	}
}
