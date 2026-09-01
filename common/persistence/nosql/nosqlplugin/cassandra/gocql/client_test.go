package gocql

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/config"
)

func Test_GetRegisteredClient(t *testing.T) {
	assert.Panics(t, func() { GetRegisteredClient() })
}

func Test_GetRegisteredClientNotNil(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	registered = NewMockClient(mockCtrl)
	assert.Equal(t, registered, GetRegisteredClient())
}

func Test_RegisterClient(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	RegisterClient(nil)
}

func Test_RegisterClientNotNil(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	newClient := NewMockClient(mockCtrl)
	registered = nil
	RegisterClient(newClient)
	assert.Equal(t, newClient, registered)
}

func Test_newCassandraCluster(t *testing.T) {
	testFullConfig := ClusterConfig{
		Hosts:      "testHost1,testHost2,testHost3,testHost4",
		Port:       123,
		User:       "testUser",
		Password:   "testPassword",
		Keyspace:   "testKeyspace",
		Datacenter: "testDatacenter",
		Region:     "testRegion",
		TLS: &config.TLS{
			Enabled:  true,
			CertFile: "testCertFile",
			KeyFile:  "testKeyFile",
		},
		MaxConns: 10,
	}
	clusterConfig := newCassandraCluster(testFullConfig)
	assert.Equal(t, []string{"testHost1", "testHost2", "testHost3", "testHost4"}, clusterConfig.Hosts)
	assert.Equal(t, testFullConfig.Port, clusterConfig.Port)
	assert.Equal(t, testFullConfig.User, clusterConfig.Authenticator.(gocql.PasswordAuthenticator).Username)
	assert.Equal(t, testFullConfig.Password, clusterConfig.Authenticator.(gocql.PasswordAuthenticator).Password)
	assert.Equal(t, testFullConfig.Keyspace, clusterConfig.Keyspace)
	assert.Equal(t, testFullConfig.TLS.CertFile, clusterConfig.SslOpts.CertPath)
	assert.Equal(t, testFullConfig.TLS.KeyFile, clusterConfig.SslOpts.KeyPath)
	assert.Equal(t, testFullConfig.MaxConns, clusterConfig.NumConns)

	assert.False(t, clusterConfig.HostFilter.Accept(&gocql.HostInfo{}))
}
