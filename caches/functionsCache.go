package caches

import (
	"config"
	"fmt"
	"slices"

	"github.com/ermes-thesis/ermes-common/caches"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"golang.org/x/sync/singleflight"
)

type FunctionCache interface {
	GetTemplate(functionName string, functionVersion string, templateName string) (*model.QueryTemplate, error)
	GetBinary(functionName string, functionVersion string) ([]byte, error)
	GetMetadata(functionName string, functionVersion string) (*model.FunctionMetadata, error)
}

type FunctionCacheImpl struct {
	Templates        *TemplatesCache
	MetadataBinaries *caches.MetadataBinaryCache
	LatestVersions   *caches.BaseCache[string]
	fetcher          caches.Fetcher[*caches.FunctionBundle]
	group            singleflight.Group
	log              logger.Logger
}

func NewFunctionCache(config config.FunctionsCacheConfig, fetcher caches.Fetcher[*caches.FunctionBundle], log logger.Logger) (*FunctionCacheImpl, error) {
	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		log.Error("Failed to create template cache", "config", fmt.Sprintf("%+v", config), "fetcher", fmt.Sprintf("%+v", fetcher), "error", err)
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := caches.NewMetadataBinaryCache(config.BinaryCache, log)
	if err != nil {
		log.Error("Failed to create metadata&binary cache", "config", fmt.Sprintf("%+v", config), "fetcher", fmt.Sprintf("%+v", fetcher), "error", err)
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}
	latest, err := caches.NewBaseCache[string](config.LatestCache.Size, config.LatestCache.Ttl)
	if err != nil {
		log.Error("Failed to create latest versions cache", "config", fmt.Sprintf("%+v", config), "fetcher", fmt.Sprintf("%+v", fetcher), "error", err)
		return nil, fmt.Errorf("error creating latest versions cache: %v", err)
	}

	return &FunctionCacheImpl{
		Templates:        tmplCache,
		MetadataBinaries: binCache,
		LatestVersions:   latest,
		fetcher:          fetcher,
		log:              log,
	}, nil
}

func NewDefaultFunctionCache(config config.FunctionsCacheConfig, ermesSecret string, log logger.Logger) (*FunctionCacheImpl, error) {
	log.Info("Initializing default FunctionCache with remote bundle fetcher", "function_registry_address", config.FunctionRegistry.Address, "function_registry_port", config.FunctionRegistry.Port)

	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		log.Error("Failed to create template cache", "config", fmt.Sprintf("%+v", config), "ermes_secret", ermesSecret, "function_registry_address", config.FunctionRegistry.Address, "function_registry_port", "error", err)
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := caches.NewMetadataBinaryCache(config.BinaryCache, log)
	if err != nil {
		log.Error("Failed to create metadata&binary cache", "config", fmt.Sprintf("%+v", config), "ermes_secret", ermesSecret, "function_registry_address", config.FunctionRegistry.Address, "function_registry_port", "error", err)
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}
	latest, err := caches.NewBaseCache[string](config.LatestCache.Size, config.LatestCache.Ttl)
	if err != nil {
		log.Error("Failed to create latest versions cache", "config", fmt.Sprintf("%+v", config), "ermes_secret", ermesSecret, "function_registry_address", config.FunctionRegistry.Address, "function_registry_port", "error", err)
		return nil, fmt.Errorf("error creating latest versions cache: %v", err)
	}

	return &FunctionCacheImpl{
		Templates:        tmplCache,
		MetadataBinaries: binCache,
		LatestVersions:   latest,
		fetcher:          caches.NewRemoteBundleFetcher[caches.FunctionBundle](fmt.Sprintf("http://%s:%s/retrieve-function", config.FunctionRegistry.Address, config.FunctionRegistry.Port), ermesSecret),
		group:            singleflight.Group{},
		log:              log,
	}, nil
}

func (f *FunctionCacheImpl) GetTemplate(functionName string, functionVersion string, templateName string) (*model.QueryTemplate, error) {
	f.log.Info("GetTemplate called", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)

	if functionVersion == "latest" {
		f.log.Debug("Resolving latest version for template", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
		bundle, err := f.resolveLatest(functionName, "template")
		if err != nil {
			f.log.Error("Failed to resolve latest template bundle", "error", err)
			return nil, err
		}

		if bundle != nil {
			q, ok := bundle.Templates[templateName]
			if ok {
				f.log.Info("Template found with latest cache", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
				return &q, nil
			}
			f.log.Warn("Requested template is not associated with the specified function", "fn_name", functionName, "fn_version", functionVersion, "fn_bundle", bundle, "template_name", templateName)
			return nil, fmt.Errorf("requested template %s not associated with function %s", templateName, functionName)
		}
		panic("This should be unreachable!!!")
	}

	if meta, ok := f.MetadataBinaries.Get(functionName, functionVersion); ok {
		f.log.Debug("Function binary and metadata found in cache", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
		if slices.Contains(meta.TemplateNames, templateName) {
			if tmpl, ok := f.Templates.Get(templateName); ok {
				f.log.Info("Template found in cache", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
				return &tmpl, nil
			}

			f.log.Debug("Cache miss: Template missing, fetching from registry", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
			fnBundle, err := f.fetchAndStore(functionName, functionVersion)
			if err != nil {
				f.log.Error("Failed to fetch and store function bundle", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName, "error", err)
				return nil, err
			}
			q := fnBundle.Templates[templateName]

			f.log.Info("Missing template has been fetched correctly and is now cached.", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
			f.log.Debug("Missing template has been fetched correctly and is now cached.", "fn_bundle", fmt.Sprintf("%+v", fnBundle), "template_content", fmt.Sprintf("%+v", q))
			return &q, nil
		} else {
			f.log.Warn("Template not associated with function", "fn_name", functionName, "fn_version", functionVersion, "fn_metadata", meta, "template_name", templateName)
			return nil, fmt.Errorf("requested template %s not associated with function %s", templateName, functionName)
		}
	} else {
		f.log.Info("Cache miss: Function metadata missing, fetching entire bundle from registry", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)

		fnBundle, err := f.fetchAndStore(functionName, functionVersion)
		if err != nil {
			f.log.Error("Failed to fetch and store function bundle", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName, "error", err)
			return nil, err
		}
		q, ok := fnBundle.Templates[templateName]
		if !ok {
			f.log.Warn("Template not found in fetched bundle", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
			return nil, fmt.Errorf("requested template %s not associated with function %s", templateName, functionName)
		}

		f.log.Info("Missing metadata has been fetched correctly. Template has been found in the retrieved function bundle", "fn_name", functionName, "fn_version", functionVersion, "template_name", templateName)
		f.log.Debug("Missing metadata has been fetched correctly. Template has been found in the retrieved function bundle", "fn_bundle", fmt.Sprintf("%+v", fnBundle), "template_content", fmt.Sprintf("%+v", q))
		return &q, nil
	}
}

func (f *FunctionCacheImpl) GetBinary(functionName string, functionVersion string) ([]byte, error) {
	f.log.Info("GetBinary called", "fn_name", functionName, "fn_version", functionVersion)

	if functionVersion == "latest" {
		f.log.Debug("Resolving latest binary", "fn_name", functionName, "fn_version", functionVersion)
		bundle, err := f.resolveLatest(functionName, "binary")
		if err != nil {
			f.log.Error("Failed to resolve latest binary bundle", "fn_name", functionName, "fn_version", functionVersion, "error", err)
			return nil, err
		}
		if bundle != nil {
			f.log.Info("Latest binary retrieved successfully", "fn_name", functionName, "fn_version", functionVersion)
			return bundle.Binary, nil
		}
		panic("This should be unreachable!!!")
	}

	bin, _, ok, err := f.MetadataBinaries.GetWithBinary(functionName, functionVersion)
	if err != nil {
		f.log.Error("Binary cache lookup failed", "fn_name", functionName, "fn_version", functionVersion, "error", err)
		return nil, err
	}
	if ok {
		f.log.Info("Binary found in cache successfully", "fn_name", functionName, "fn_version", functionVersion)
		return bin, nil
	}

	f.log.Debug("Cache miss: fetching entire function bundle from registry", "fn_name", functionName, "fn_version", functionVersion)
	fnBundle, err := f.fetchAndStore(functionName, functionVersion)
	if err != nil {
		f.log.Error("Failed to fetch and store function bundle", "fn_name", functionName, "fn_version", functionVersion, "error", err)
		return nil, err
	}

	f.log.Info("Missing binary has been fetched correctly", "fn_name", functionName, "fn_version", functionVersion)
	f.log.Info("Missing binary has been fetched correctly", "fn_bundle", fmt.Sprintf("%+v", fnBundle))
	return fnBundle.Binary, err
}

func (f *FunctionCacheImpl) GetMetadata(functionName string, functionVersion string) (*model.FunctionMetadata, error) {
	f.log.Info("GetMetadata called", "fn_name", functionName, "fn_version", functionVersion)

	if functionVersion == "latest" {
		f.log.Debug("Resolving latest metadata", "fn_name", functionName, "fn_version", functionVersion)
		bundle, err := f.resolveLatest(functionName, "metadata")
		if err != nil {
			f.log.Error("Failed to resolve latest metadata", "fn_name", functionName, "fn_version", functionVersion, "error", err)
			return nil, err
		}
		if bundle != nil {
			f.log.Info("Latest metadata retrieved successfully", "fn_name", functionName, "fn_version", functionVersion)
			return &bundle.Metadata, nil
		}
		panic("This should be unreachable!!!")
	}

	meta, ok := f.MetadataBinaries.Get(functionName, functionVersion)
	if ok {
		f.log.Info("Metadata found in cache successfully", "fn_name", functionName, "fn_version", functionVersion, "metadata", fmt.Sprintf("%+v", meta))
		return &meta, nil
	}

	f.log.Debug("Cache miss: fetching entire function bundle from registry", "fn_name", functionName, "fn_version", functionVersion)
	fnBundle, err := f.fetchAndStore(functionName, functionVersion)
	if err != nil {
		f.log.Error("Failed to fetch and store function bundle", "fn_name", functionName, "fn_version", functionVersion, "error", err)
		return nil, err
	}

	f.log.Info("Missing metadata has been fetched correctly", "fn_name", functionName, "fn_version", functionVersion)
	f.log.Info("Missing metadata has been fetched correctly", "fn_bundle", fmt.Sprintf("%+v", fnBundle))
	return &fnBundle.Metadata, err
}

func (f *FunctionCacheImpl) getSnapshot(functionName string, functionVersion string, want string) (*caches.FunctionBundle, error) {
	f.log.Debug("Creating cache snapshot", "fn_name", functionName, "fn_version", functionVersion, "part_wanted", want)

	switch want {
	case "metadata":
		if meta, ok := f.MetadataBinaries.Get(functionName, functionVersion); ok {
			if f.log.Enabled(logger.DebugLevel) {
				f.log.Debug("Returning metadata snapshot", "fn_name", functionName, "fn_version", functionVersion, "metadata", fmt.Sprintf("%+v", meta))
			}
			return &caches.FunctionBundle{Metadata: meta}, nil
		}

	case "binary":
		if bin, _, okBin, err := f.MetadataBinaries.GetWithBinary(functionName, functionVersion); okBin && bin != nil {
			if f.log.Enabled(logger.DebugLevel) {
				f.log.Debug("Returning binary snapshot", "fn_name", functionName, "fn_version", functionVersion, "binary", slices.Clone(bin[:min(len(bin), 30)]))
			}
			return &caches.FunctionBundle{Binary: bin}, err
		}

	case "template":
		if meta, ok := f.MetadataBinaries.Get(functionName, functionVersion); ok {
			all := true
			tmpls := make(map[string]model.QueryTemplate)
			for _, tname := range meta.TemplateNames {
				if t, okT := f.Templates.Get(tname); okT {
					tmpls[tname] = t
				} else {
					all = false
					break
				}
			}
			if all {
				if f.log.Enabled(logger.DebugLevel) {
					f.log.Debug("Returning templates snapshot", "fn_name", functionName, "fn_version", functionVersion, "templates", fmt.Sprintf("%+v", tmpls))
				}
				return &caches.FunctionBundle{Templates: tmpls}, nil
			}
		}
	default:
		f.log.Error("Unknown snapshot type requested", "fn_name", functionName, "fn_version", functionVersion, "part_wanted", want)
		return nil, fmt.Errorf("unknown want type: %s", want)
	}

	f.log.Warn("The searched part was not found in the relative cache. Couldn't perform the snapshot", "fn_name", functionName, "fn_version", functionVersion, "part_wanted", want)
	return nil, nil
}

func (f *FunctionCacheImpl) resolveLatest(functionName, want string) (*caches.FunctionBundle, error) {
	f.log.Debug("Resolving latest", "fn_name", functionName, "part_wanted", want)

	cachedVersion, found, expired := f.LatestVersions.GetWithStatus(functionName)
	f.log.Debug("Search in latest versions cache results", "fn_name", functionName, "retrieved_cached_version", cachedVersion, "expired", expired)
	bundle, err := f.getSnapshot(functionName, cachedVersion, want)
	if err != nil {
		f.log.Error("Failed to get snapshot", "fn_name", functionName, "part_wanted", want, "error", err)
		return nil, err
	}

	key := functionName + "@latest"
	if bundle == nil {
		fetched, err, _ := f.group.Do(key, func() (interface{}, error) {
			f.log.Debug("Cache miss: Missing latest version function bundle", "fn_name", functionName, "part_wanted", want)
			fetchedBundle, err := f.fetcher.Fetch(functionName, "latest")
			if err != nil {
				f.log.Error("Failed to fetch latest bundle", "fn_name", functionName, "part_wanted", want, "error", err)
				return nil, err
			}

			f.log.Debug("Function bundle fetched correctly, proceeding to store it in the caches", "fn_name", functionName, "part_wanted", want, "fn_bundle", fmt.Sprintf("%+v", fetchedBundle))
			f.LatestVersions.Set(functionName, fetchedBundle.Metadata.Version)
			err = f.store(fetchedBundle)
			if err != nil {
				f.log.Error("Failed to store function bundle", "fn_name", functionName, "part_wanted", want, "fn_bundle", fmt.Sprintf("%+v", fetchedBundle), "error", err)
				return nil, err
			}

			f.log.Debug("Function bundle stored successfully", "fn_name", functionName, "part_wanted", want, "fn_bundle", fmt.Sprintf("%+v", fetchedBundle))
			return fetchedBundle, nil
		})
		if err != nil {
			f.log.Error("Failed to fetch bundle", "fn_name", functionName, "fn_version", "latest", "want", want, "error", err)
			return nil, err
		}
		return fetched.(*caches.FunctionBundle), nil

	} else {
		if found && !expired {
			f.log.Debug("Latest version found in cache, and it's NOT expired", "fn_name", functionName, "part_wanted", want)
			return bundle, nil
		}

		fetched, err, _ := f.group.Do(key, func() (interface{}, error) {
			f.log.Debug("Latest version found in cache but it's expired. Refreshing it in the cache", "fn_name", functionName, "part_wanted", want)
			fetchedBundle, err := f.fetcher.FetchLatest(functionName, cachedVersion)

			if fetchedBundle != nil {
				f.log.Debug("Function bundle fetched correctly, proceeding to store it in the caches", "fn_name", functionName, "part_wanted", want, "fn_bundle", fmt.Sprintf("%+v", fetchedBundle))
				f.LatestVersions.Set(functionName, fetchedBundle.Metadata.Version)
				err = f.store(fetchedBundle)
				if err != nil {
					f.log.Error("Failed to store function bundle", "fn_name", functionName, "part_wanted", want, "fn_bundle", fmt.Sprintf("%+v", fetchedBundle), "error", err)
					return nil, err
				}

				f.log.Debug("Function bundle stored successfully", "fn_name", functionName, "part_wanted", want, "fn_bundle", fmt.Sprintf("%+v", fetchedBundle))
				return fetchedBundle, nil
			}

			// refresh latest cache
			f.LatestVersions.Set(functionName, cachedVersion)
			return bundle, err
		})
		if err != nil {
			f.log.Error("Failed to fetch bundle", "fn_name", functionName, "fn_version", "latest", "want", want, "error", err)
			return nil, err
		}
		return fetched.(*caches.FunctionBundle), nil
	}
}

func (f *FunctionCacheImpl) store(bundle *caches.FunctionBundle) error {
	f.log.Debug("Storing bundle in caches", "fn_bundle", fmt.Sprintf("%+v", bundle))
	for name, template := range bundle.Templates {
		f.Templates.Set(name, template)
	}
	if err := f.MetadataBinaries.Set(bundle.Metadata, bundle.Binary); err != nil {
		f.log.Error("Failed to store bundle in MetadataBinaryCache", "fn_bundle", fmt.Sprintf("%+v", bundle), "error", err)
		return err
	}
	return nil
}

func (f *FunctionCacheImpl) fetchAndStore(functionName string, functionVersion string) (*caches.FunctionBundle, error) {
	f.log.Debug("Fetching bundle and storing function bundle", "fn_name", functionName, "fn_version", functionVersion)

	if functionVersion == "latest" {
		f.log.Error("FAILURE: Reached a code part that should be unreachable", "fn_name", functionName, "fn_version", functionVersion)
		panic("This should be unreachable!!!")
	}

	key := functionName + "@" + functionVersion
	fetchedValue, err, _ := f.group.Do(key, func() (any, error) {
		f.log.Debugf("Singleflight: performing fetch for key: %s", key)
		bundle, err := f.fetcher.Fetch(functionName, functionVersion)
		if err != nil {
			f.log.Error("Failed to fetch bundle", "fn_name", functionName, "fn_version", functionVersion, "error", err)
			return nil, err
		}

		if bundle == nil || len(bundle.Binary) == 0 || bundle.Templates == nil {
			f.log.Error("Invalid bundle fetched", "fn_name", functionName, "fn_version", functionVersion, "fn_bundle", fmt.Sprintf("%+v", bundle))
			return nil, fmt.Errorf("invalid bundle from registry")
		}

		err = f.store(bundle)
		if err != nil {
			return nil, err
		}

		f.log.Debug("Function bundle fetched and stored successfully", "fn_name", functionName, "fn_version", functionVersion)
		return bundle, nil
	})

	if err != nil {
		f.log.Error("Failed to fetch bundle", "fn_name", functionName, "fn_version", functionVersion, "error", err)
		return nil, err
	}
	return fetchedValue.(*caches.FunctionBundle), nil
}
