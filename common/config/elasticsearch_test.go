package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSSigning_ValidateEmpty(t *testing.T) {

	tests := []struct {
		msg    string
		config AWSSigning
		err    error
	}{
		{
			msg: "Empty config should error",
			config: AWSSigning{
				StaticCredential:      nil,
				EnvironmentCredential: nil,
			},
			err: errAWSSigningCredential,
		},
		{
			msg: "error when both config sections are provided",
			config: AWSSigning{
				Enable:                false,
				StaticCredential:      &AWSStaticCredential{},
				EnvironmentCredential: &AWSEnvironmentCredential{},
			},
			err: errAWSSigningCredential,
		},
		{
			msg: "StaticCredential must have region set",
			config: AWSSigning{
				Enable:                false,
				StaticCredential:      &AWSStaticCredential{},
				EnvironmentCredential: nil,
			},
			err: errors.New("missing region in staticCredential"),
		},
		{
			msg: "EnvironmentCredential must have region set",
			config: AWSSigning{
				Enable:                false,
				StaticCredential:      nil,
				EnvironmentCredential: &AWSEnvironmentCredential{},
			},
			err: errors.New("missing region in environmentCredential"),
		},
		{
			msg: "Valid StaticCredential config should have no error ",
			config: AWSSigning{
				Enable:                false,
				StaticCredential:      &AWSStaticCredential{Region: "region1"},
				EnvironmentCredential: nil,
			},
			err: nil,
		},
		{
			msg: "Valid EnvironmentCredential config should have no error",
			config: AWSSigning{
				Enable:                false,
				StaticCredential:      nil,
				EnvironmentCredential: &AWSEnvironmentCredential{Region: "region1"},
			},
			err: nil,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.err, tc.config.Validate(), tc.msg)
	}

}

func TestGetCustomHeader(t *testing.T) {

	tests := []struct {
		config   ElasticSearchConfig
		header   string
		expected string
	}{
		{
			config: ElasticSearchConfig{
				CustomHeaders: map[string]string{
					"key1": "value1",
				},
			},
			header:   "key1",
			expected: "value1",
		},
		{
			config: ElasticSearchConfig{
				CustomHeaders: map[string]string{
					"key1": "value1",
				},
			},
			header:   "key2",
			expected: "",
		},
	}

	for _, tc := range tests {
		val := tc.config.GetCustomHeader(tc.header)
		assert.Equal(t, val, tc.expected)
	}

}
