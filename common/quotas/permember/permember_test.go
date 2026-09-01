package permember

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/membership"
)

func Test_PerMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)
	resolver.EXPECT().MemberCount("A").Return(10, nil).MinTimes(1)
	resolver.EXPECT().MemberCount("X").Return(0, assert.AnError).MinTimes(1)
	resolver.EXPECT().MemberCount("Y").Return(0, nil).MinTimes(1)

	// Invalid service - fallback to instanceRPS
	assert.Equal(t, 3.0, PerMember("X", 20.0, 3.0, resolver))

	// Invalid member count - fallback to instanceRPS
	assert.Equal(t, 3.0, PerMember("Y", 20.0, 3.0, resolver))

	// GlobalRPS not provided - fallback to instanceRPS
	assert.Equal(t, 3.0, PerMember("A", 0, 3.0, resolver))

	// Calculate average per member RPS (prefer averaged global - lower)
	assert.Equal(t, 2.0, PerMember("A", 20.0, 3.0, resolver))

	// Calculate average per member RPS (prefer instanceRPS - lower)
	assert.Equal(t, 3.0, PerMember("A", 100.0, 3.0, resolver))
}

func Test_PerMemberFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)
	resolver.EXPECT().MemberCount("A").Return(10, nil).MinTimes(1)

	factory := NewPerMemberDynamicRateLimiterFactory(
		"A",
		func(string) int { return 20 },
		func(string) int { return 3 },
		resolver,
	)

	limiter := factory.GetLimiter("TestDomainName")

	// The limit is 20 and there are 10 instances, so the per member limit is 2
	assert.Equal(t, true, limiter.Allow())
	assert.Equal(t, true, limiter.Allow())
	assert.Equal(t, false, limiter.Allow())
}
