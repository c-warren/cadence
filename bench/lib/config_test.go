package lib

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ConfigTestSuite struct {
	suite.Suite
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}

func (s *ConfigTestSuite) TestValidate() {
	testCases := []func(*Config){
		func(c *Config) { c.Bench.Name = "" },
		func(c *Config) { c.Bench.Domains = []string{} },
		func(c *Config) { c.Bench.NumTaskLists = 0 },
	}

	for _, tc := range testCases {
		config := s.buildConfig()
		tc(&config)
		s.Error(config.Validate())
	}
}

func (s *ConfigTestSuite) buildConfig() Config {
	return Config{
		Bench: Bench{
			Name:         "cadence-bench",
			Domains:      []string{"cadence-bench"},
			NumTaskLists: 1,
		},
	}
}
