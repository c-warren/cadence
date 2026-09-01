package config

// PinotVisibilityConfig for connecting to Pinot
type (
	PinotVisibilityConfig struct {
		Cluster     string              `yaml:"cluster"`     //nolint:govet
		Broker      string              `yaml:"broker"`      //nolint:govet
		Table       string              `yaml:"table"`       //nolint:govet
		ServiceName string              `yaml:"serviceName"` //nolint:govet
		Migration   VisibilityMigration `yaml:"migration"`   //nolint:govet
	}
)
