package domaindeprecation

// DomainDeprecationParams contains the parameters required for domain deprecation workflow.
type DomainDeprecationParams struct {
	DomainName    string `json:"domain_name"`
	SecurityToken string `json:"security_token"`
	Force         bool   `json:"force"`
}
