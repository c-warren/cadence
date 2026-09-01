package cache

type noOpDomainCache struct{}

func (c *noOpDomainCache) GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64) {
	return 0, 0
}

func (c *noOpDomainCache) GetAllDomain() map[string]*DomainCacheEntry {
	return map[string]*DomainCacheEntry{}
}

func (c *noOpDomainCache) RegisterDomainChangeCallback(
	id string,
	catchUpFn CatchUpFn,
	prepareCallback PrepareCallbackFn,
	callback CallbackFn,
) {
}

func (c *noOpDomainCache) UnregisterDomainChangeCallback(
	id string,
) {
}

func (c *noOpDomainCache) GetDomain(
	name string,
) (*DomainCacheEntry, error) {
	return &DomainCacheEntry{}, nil
}

func (c *noOpDomainCache) GetDomainByID(
	id string,
) (*DomainCacheEntry, error) {
	return &DomainCacheEntry{}, nil
}

func (c *noOpDomainCache) GetDomainID(
	name string,
) (string, error) {
	return "", nil
}

func (c *noOpDomainCache) GetDomainName(
	id string,
) (string, error) {
	return "", nil
}

func (c *noOpDomainCache) getDomain(
	name string,
) (*DomainCacheEntry, error) {
	return &DomainCacheEntry{}, nil
}

func (c *noOpDomainCache) getDomainByID(
	id string,
	deepCopy bool,
) (*DomainCacheEntry, error) {
	return &DomainCacheEntry{}, nil
}

func NewNoOpDomainCache() DomainCache {
	return &noOpDomainCache{}
}

func (c *noOpDomainCache) Start() {}

func (c *noOpDomainCache) Stop() {}
