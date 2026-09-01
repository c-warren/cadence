package schema

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type VersionGraphTestSuite struct {
	suite.Suite
}

func TestVersionGraphTestSuite(t *testing.T) {
	suite.Run(t, new(VersionGraphTestSuite))
}

func (s *VersionGraphTestSuite) TestFindShortestPath() {
	increments := []string{"v0.1", "v0.2", "v0.3", "v0.4", "v0.5", "v1.0", "v1.1", "v1.2", "v1.3"}
	tests := []struct {
		name      string
		to        string
		shortcuts []squashVersion
		want      []string
	}{
		{
			name: "increments only",
			want: []string{"v0.1", "v0.2", "v0.3", "v0.4", "v0.5", "v1.0", "v1.1", "v1.2", "v1.3"},
		},
		{
			name:      "single hop",
			shortcuts: []squashVersion{{prev: "0.0", ver: "1.3", dirName: "foo"}},
			want:      []string{"foo"},
		},
		{
			name: "middle hops",
			shortcuts: []squashVersion{
				{prev: "0.2", ver: "0.4", dirName: "foo"},
				{prev: "1.0", ver: "1.2", dirName: "bar"},
			},
			want: []string{"v0.1", "v0.2", "foo", "v0.5", "v1.0", "bar", "v1.3"},
		},
		{
			name: "hop at the start",
			shortcuts: []squashVersion{
				{prev: "0.0", ver: "0.4", dirName: "foo"},
			},
			want: []string{"foo", "v0.5", "v1.0", "v1.1", "v1.2", "v1.3"},
		},
		{
			name: "hop at the end",
			shortcuts: []squashVersion{
				{prev: "1.0", ver: "1.3", dirName: "foo"},
			},
			want: []string{"v0.1", "v0.2", "v0.3", "v0.4", "v0.5", "v1.0", "foo"},
		},
		{
			name: "out of range",
			to:   "v1.4",
			shortcuts: []squashVersion{
				{prev: "1.0", ver: "1.3", dirName: "foo"},
			},
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			to := increments[len(increments)-1]
			if tt.to != "" {
				to = tt.to
			}
			p, err := findShortestPath("0.0", dirToVersion(to), increments, tt.shortcuts)
			s.Require().NoError(err)
			s.Equal(tt.want, p)
		})
	}
}
