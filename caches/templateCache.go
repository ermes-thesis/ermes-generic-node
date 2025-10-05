package caches

import (
	"config"

	"github.com/ermes-thesis/ermes-common/model"
)

// TemplatesCache stores compiled templates for functions. Each function
// name maps to a dictionary of template names and their corresponding structure.
//
// Internally, it uses a BaseCache that maps function names to a map[string]string
// containing template name-value pairs.
type TemplatesCache struct {
	templates *BaseCache[model.QueryTemplate]
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
	templatesCache, err := NewBaseCache[model.QueryTemplate](config.Size, config.Ttl)
	if err != nil {
		return nil, err
	}

	return &TemplatesCache{
		templates: templatesCache,
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
func (t *TemplatesCache) Get(templateName string) (model.QueryTemplate, bool) {
	return t.templates.Get(templateName)
}

// Set stores the templates for a given function name.
//
// Parameters:
//   - functionName: the name of the function to associate with the templates
//   - templates: a map of template names to template values
func (t *TemplatesCache) Set(templateName string, template model.QueryTemplate) {
	t.templates.Set(templateName, template)
}
