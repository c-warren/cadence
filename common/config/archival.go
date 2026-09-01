package config

import (
	"errors"

	"github.com/uber/cadence/common/constants"
)

// Validate validates the archival config
func (a *Archival) Validate(domainDefaults *ArchivalDomainDefaults) error {
	if a.History.Status == constants.ArchivalEnabled {
		if domainDefaults.History.URI == "" || a.History.Provider == nil {
			return errors.New("invalid history archival config, must provide domainDefaults.History.URI and Provider")
		}
	} else {
		if a.History.EnableRead {
			return errors.New("invalid history archival config, cannot EnableRead when archival is disabled")
		}
	}

	if a.Visibility.Status == constants.ArchivalEnabled {
		if domainDefaults.Visibility.URI == "" || a.Visibility.Provider == nil {
			return errors.New("invalid visibility archival config, must provide domainDefaults.Visibility.URI and Provider")
		}
	} else {
		if a.Visibility.EnableRead {
			return errors.New("invalid visibility archival config, cannot EnableRead when archival is disabled")
		}
	}

	return nil
}
