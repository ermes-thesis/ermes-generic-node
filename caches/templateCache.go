package caches

import (
	"config"
)

// TODO: In the template cache we store functionName -> [<tmpl1_name, tmpl1_value>, <tmpl2_name, tmpl2_value>, ...]
//
//	or tmpl1_name -> tmpl1_value, tmpl2_name -> tmpl2_value, ...
type TemplatesCache struct {
	cache *BaseCache[map[string]string]
}

func NewTemplatesCache(config config.TemplatesCacheConfig) (*TemplatesCache, error) {
	cache, err := NewBaseCache[map[string]string](config.Size, config.Ttl)
	if err != nil {
		return nil, err
	}

	return &TemplatesCache{
		cache: cache,
	}, nil
}

func (t *TemplatesCache) Get(functionName string) (map[string]string, bool) {
	return t.cache.Get(functionName)
}

func (t *TemplatesCache) Set(functionName string, templates map[string]string) {
	t.cache.Set(functionName, templates)
}
