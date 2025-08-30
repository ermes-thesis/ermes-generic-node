package caches

import (
	"config"
)

// TODO: In the template cache we store functionName -> [<tmpl1_name, tmpl1_value>, <tmpl2_name, tmpl2_value>, ...]
//
//	or tmpl1_name -> tmpl1_value, tmpl2_name -> tmpl2_value, ...

// TemplatesCache stores compiled templates for functions. Each function
// name maps to a dictionary of template names and their corresponding structure.
//
// Internally, it uses a BaseCache that maps function names to a map[string]string
// containing template name-value pairs.
type TemplatesCache struct {
	cache *BaseCache[map[string]string]
}

// NewTemplatesCache creates a new TemplatesCache based on the given configuration.
//
// Parameters:
//   - config: config.TemplatesCacheConfig containing the cache size and TTL
//
// Returns:
//   - *TemplatesCache: the newly created templates cache
//   - error: non-nil if the underlying BaseCache could not be created
func NewTemplatesCache(config config.TemplatesCacheConfig) (*TemplatesCache, error) {
	cache, err := NewBaseCache[map[string]string](config.Size, config.Ttl)
	if err != nil {
		return nil, err
	}

	return &TemplatesCache{
		cache: cache,
	}, nil
}

// Get retrieves the templates associated with the given function name.
//
// Parameters:
//   - functionName: the name of the function whose templates should be retrieved
//
// Returns:
//   - map[string]string: the templates associated with the function (nil if not found)
//   - bool: true if templates were found, false otherwise
func (t *TemplatesCache) Get(functionName string) (map[string]string, bool) {
	return t.cache.Get(functionName)
}

// Set stores the templates for a given function name.
//
// Parameters:
//   - functionName: the name of the function to associate with the templates
//   - templates: a map of template names to template values
func (t *TemplatesCache) Set(functionName string, templates map[string]string) {
	t.cache.Set(functionName, templates)
}
