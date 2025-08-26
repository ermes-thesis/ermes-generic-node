package caches

import (
	"config"
)

type TemplatesCache struct {
	cache *BaseCache[[]string]
}

func NewTemplatesCache(config config.TemplatesCacheConfig) (*TemplatesCache, error) {
	cache, err := NewBaseCache[[]string](config.Size, config.Ttl)
	if err != nil {
		return nil, err
	}

	return &TemplatesCache{
		cache: cache,
	}, nil
}

func (t *TemplatesCache) Get(functionName string) ([]string, bool) {
	return t.cache.Get(functionName)
}

func (t *TemplatesCache) Set(functionName string, templates []string) {
	t.cache.Set(functionName, templates)
}
