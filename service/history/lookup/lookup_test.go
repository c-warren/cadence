package lookup

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/service"
)

func TestHistoryServerByShardID_Succeeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockResolver := membership.NewMockResolver(ctrl)

	mockResolver.EXPECT().Lookup(service.History, string(rune(65))).
		Return(membership.NewHostInfo("127.0.0.1:1234"), nil)

	host, err := HistoryServerByShardID(mockResolver, 65)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:1234", host.GetAddress())
}

func TestHistoryServerByShardID_PreservesError(t *testing.T) {
	lookupError := errors.New("lookup failed")
	ctrl := gomock.NewController(t)
	mockResolver := membership.NewMockResolver(ctrl)

	mockResolver.EXPECT().Lookup(service.History, gomock.Any()).
		Return(membership.HostInfo{}, lookupError)

	host, err := HistoryServerByShardID(mockResolver, 65)
	assert.Equal(t, lookupError, err, "error should not be modified")
	assert.Empty(t, host)
}
