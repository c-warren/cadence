package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/config/yaml"
	"github.com/uber/cadence/common/constants"
)

func defaultFilestoreConfig(t *testing.T) *yaml.Node {
	node, err := yaml.ToNode(&FilestoreArchiver{
		FileMode: "044",
	})
	require.NoError(t, err)
	return node
}

// History archival

func TestValidEnabledHistoryArchivalConfig(t *testing.T) {
	archival := Archival{
		History: HistoryArchival{
			Status: constants.ArchivalEnabled,
			Provider: HistoryArchiverProvider{
				FilestoreConfig: defaultFilestoreConfig(t),
			},
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{
		History: HistoryArchivalDomainDefaults{
			URI: "/var/tmp",
		},
	})
	require.NoError(t, err)
}

func TestInvalidHEnabledHistoryArchivalConfig(t *testing.T) {
	archival := Archival{
		History: HistoryArchival{
			Status: constants.ArchivalEnabled,
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.Error(t, err)
}

func TestValidDisabledHistoryArchivalConfig(t *testing.T) {
	archival := Archival{
		History: HistoryArchival{
			Provider: HistoryArchiverProvider{
				FilestoreConfig: defaultFilestoreConfig(t),
			},
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.NoError(t, err)
}

func TestInvalidDisabledHistoryArchivalConfig(t *testing.T) {
	archival := Archival{
		History: HistoryArchival{
			EnableRead: true,
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.Error(t, err)
}

func TestValidEmptyHistoryArchivalConfig(t *testing.T) {
	archival := Archival{
		History: HistoryArchival{},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.NoError(t, err)
}

// Visibility archival

func TestValidEnabledVisibilityArchivalConfig(t *testing.T) {
	archival := Archival{
		Visibility: VisibilityArchival{
			Status: constants.ArchivalEnabled,
			Provider: VisibilityArchiverProvider{
				FilestoreConfig: defaultFilestoreConfig(t),
			},
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{
		Visibility: VisibilityArchivalDomainDefaults{
			URI: "/var/tmp",
		},
	})
	require.NoError(t, err)
}

func TestInvalidHEnabledVisibilityArchivalConfig(t *testing.T) {
	archival := Archival{
		Visibility: VisibilityArchival{
			Status: constants.ArchivalEnabled,
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.Error(t, err)
}

func TestValidDisabledVisibilityArchivalConfig(t *testing.T) {
	archival := Archival{
		Visibility: VisibilityArchival{
			Provider: VisibilityArchiverProvider{
				FilestoreConfig: defaultFilestoreConfig(t),
			},
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.NoError(t, err)
}

func TestInvalidDisabledVisibilityArchivalConfig(t *testing.T) {
	archival := Archival{
		Visibility: VisibilityArchival{
			EnableRead: true,
		},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.Error(t, err)
}

func TestValidEmptyVisibilityArchivalConfig(t *testing.T) {
	archival := Archival{
		Visibility: VisibilityArchival{},
	}
	err := archival.Validate(&ArchivalDomainDefaults{})
	require.NoError(t, err)
}
