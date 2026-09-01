package membership

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBelongs(t *testing.T) {

	host := HostInfo{}

	belongs, err := host.Belongs("127.1")
	assert.False(t, belongs, "invalid host info data should result to false")
	assert.Error(t, err)

	host2 := NewDetailedHostInfo("127.0.0.1:123", "dummy", PortMap{})
	belongs, err = host2.Belongs("127.0.0.1:123")
	assert.True(t, belongs, "match on address, port map might be empty")
	assert.NoError(t, err)

	host3 := NewDetailedHostInfo("127.0.0.1:1234", "dummy", PortMap{PortGRPC: 3333})
	belongs, err = host3.Belongs("127.0.0.1:3333")
	assert.True(t, belongs, "portmap should be checked")
	assert.NoError(t, err)

	host4 := NewDetailedHostInfo("127.0.0.1:1234", "dummy", PortMap{PortGRPC: 3333})
	belongs, err = host4.Belongs("127.0.0.2:3333")
	assert.False(t, belongs, "different IP, will result in false")
	assert.NoError(t, err)

	host5 := NewDetailedHostInfo("127.0.0.1:1234", "dummy", PortMap{PortGRPC: 3333})
	belongs, err = host5.Belongs("127.0.0.1:3334")
	assert.False(t, belongs, "portmap has no such port, should return empty without an error")
	assert.NoError(t, err)
}
