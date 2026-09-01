package membership

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

var testServices = []string{"test-worker", "test-services"}

func TestSubscribeIsCalledOnPeerProvider(t *testing.T) {
	r, pp := newTestResolver(t)
	_, err := r.getRing("test-worker")
	assert.NoError(t, err)

	// After membership is started, we expect start, subscribe and GetMembers on PeerProvider
	pp.EXPECT().Start(gomock.Any()).Times(1).Return(nil)
	pp.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Times(len(testServices))
	pp.EXPECT().GetMembers(gomock.Any()).Times(len(testServices))

	r.Start()
}

func TestNewCreatesAllRings(t *testing.T) {
	a, _ := newTestResolver(t)
	assert.Equal(t, len(testServices), len(a.rings))

}

func TestMethodsAreRoutedToARing(t *testing.T) {
	var changeCh = make(chan *ChangedEvent)
	a, pp := newTestResolver(t)

	// add members to this ring
	hosts := []HostInfo{}
	for _, addr := range []string{"127", "128"} {
		hosts = append(hosts, NewHostInfo(addr))
	}

	pp.EXPECT().GetMembers("test-worker").Return(hosts, nil).Times(1)
	pp.EXPECT().WhoAmI().AnyTimes()

	r, err := a.getRing("test-worker")
	r.Refresh()

	assert.NoError(t, err)

	hi, err := r.Lookup("key")
	assert.NoError(t, err)
	assert.Equal(t, "127", hi.GetAddress())

	// the same ring will be picked here
	_, err = a.Lookup("WRONG-RING-NAME", "key")
	assert.Error(t, err)

	members, err := a.Members("test-worker")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	nomembers, err := a.Members("WRONG-RING-NAME")
	assert.Error(t, err)
	assert.Equal(t, 0, len(nomembers))

	memcount, err := a.MemberCount("test-worker")
	assert.NoError(t, err)
	assert.Equal(t, 2, memcount)

	nomemcount, err := a.MemberCount("WRONG-RING-NAME")
	assert.Error(t, err)
	assert.Equal(t, 0, nomemcount)

	serr := a.Subscribe("test-worker", "sub1", changeCh)
	assert.NoError(t, serr)

	serr = a.Subscribe("WRONG-RING-NAME", "sub1", changeCh)
	assert.Error(t, serr)

	serr = a.Unsubscribe("test-worker", "sub1")
	assert.NoError(t, serr)

	serr = a.Unsubscribe("WRONG-RING-NAME", "sub1")
	assert.Error(t, serr)

}

func TestNonExistingRingReturnsError(t *testing.T) {
	a, _ := newTestResolver(t)
	_, err := a.getRing("non-existing")
	assert.Error(t, err)
}

func TestCallsAreForwardedToProvider(t *testing.T) {
	a, mockedPeer := newTestResolver(t)

	mockedPeer.EXPECT().WhoAmI().Times(1)
	mockedPeer.EXPECT().SelfEvict().Times(1)
	mockedPeer.EXPECT().Stop(gomock.Any()).Times(1).Return(nil)

	a.status = common.DaemonStatusStarted
	a.WhoAmI()
	a.EvictSelf()
	a.Stop()

}

func newTestResolver(t *testing.T) (*MultiringResolver, *MockPeerProvider) {

	ctrl := gomock.NewController(t)
	pp := NewMockPeerProvider(ctrl)

	rings := make(map[string]SingleProvider, len(testServices))
	for _, testService := range testServices {
		ring := NewHashring(testService, pp, clock.NewMockedTimeSource(), log.NewNoop(), metrics.NewNoopMetricsClient().Scope(metrics.HashringScope))
		rings[testService] = ring
	}

	resolver, err := NewResolver(
		pp,
		metrics.NewNoopMetricsClient(),
		log.NewNoop(),
		rings,
	)
	require.NoError(t, err)

	return resolver.(*MultiringResolver), pp
}
