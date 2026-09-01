package config

type (
	// SASL describe SASL configuration (for Kafka)
	SASL struct {
		Enabled   bool   `yaml:"enabled"` // false as default
		User      string `yaml:"user"`
		Password  string `yaml:"password"`
		Algorithm string `yaml:"algorithm"` // plain, sha512 or sha256
	}
)
