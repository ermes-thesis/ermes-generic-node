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
	fetcher          caches.Fetcher[*FunctionBundle]
	group            singleflight.Group
	log              *logger.Logger
}

type FunctionBundle struct {
	Binary    []byte
	Metadata  model.FunctionMetadata
	Templates map[string]model.QueryTemplate
}

func NewFunctionCache(config config.FunctionsCacheConfig, fetcher caches.Fetcher[*FunctionBundle], log *logger.Logger) (*FunctionCacheImpl, error) {
	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := caches.NewMetadataBinaryCache(config.BinaryCache)
	if err != nil {
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}
	latest, err := caches.NewBaseCache[string](config.BinaryCache.Size, config.LatestCacheTtl)
	if err != nil {
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

func NewDefaultFunctionCache(config config.FunctionsCacheConfig, ermesSecret string, log *logger.Logger) (*FunctionCacheImpl, error) {
	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := caches.NewMetadataBinaryCache(config.BinaryCache)
	if err != nil {
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}
	latest, err := caches.NewBaseCache[string](config.BinaryCache.Size, config.LatestCacheTtl)
	if err != nil {
		return nil, fmt.Errorf("error creating latest versions cache: %v", err)
	}

	return &FunctionCacheImpl{
		Templates:        tmplCache,
		MetadataBinaries: binCache,
		LatestVersions:   latest,
		fetcher:          caches.NewRemoteBundleFetcher[FunctionBundle](fmt.Sprintf("http://%s:%s", config.FunctionRegistry.Address, config.FunctionRegistry.Port), ermesSecret),
		group:            singleflight.Group{},
		log:              log,
	}, nil
}

func (f *FunctionCacheImpl) GetTemplate(functionName string, functionVersion string, templateName string) (*model.QueryTemplate, error) {
	if functionVersion == "latest" {
		bundle, err := f.resolveLatest(functionName, "template")
		if err != nil {
			return nil, err
		}
		if bundle != nil {
			q, ok := bundle.Templates[templateName]
			if ok {
				return &q, nil
			}
			return nil, fmt.Errorf("requested template %s not associated with function %s", templateName, functionName)
		}
		panic("This should be unreachable!!!")
	}

	if meta, ok := f.MetadataBinaries.Get(functionName, functionVersion); ok {
		if slices.Contains(meta.TemplateNames, templateName) {
			if tmpl, ok := f.Templates.Get(templateName); ok {
				return &tmpl, nil
			}
			fnBundle, err := f.fetchAndStore(functionName, functionVersion)
			if err != nil {
				return nil, err
			}

			q := fnBundle.Templates[templateName]
			return &q, nil
		} else {
			return nil, fmt.Errorf("requested template %s not associated with function %s", templateName, functionName)
		}
	} else {
		fnBundle, err := f.fetchAndStore(functionName, functionVersion)
		if err != nil {
			return nil, err
		}
		q, ok := fnBundle.Templates[templateName]
		if !ok {
			return nil, fmt.Errorf("requested template %s not associated with function %s", templateName, functionName)
		}
		return &q, nil
	}
}

func (f *FunctionCacheImpl) GetBinary(functionName string, functionVersion string) ([]byte, error) {
	if functionVersion == "latest" {
		bundle, err := f.resolveLatest(functionName, "binary")
		if err != nil {
			return nil, err
		}
		if bundle != nil {
			return bundle.Binary, nil
		}
		panic("This should be unreachable!!!")
	}

	bin, _, ok, err := f.MetadataBinaries.GetWithBinary(functionName, functionVersion)
	if err != nil {
		return nil, err
	}
	if ok {
		return bin, nil
	}

	fnBundle, err := f.fetchAndStore(functionName, functionVersion)
	if err != nil {
		return nil, err
	}
	return fnBundle.Binary, err
}

func (f *FunctionCacheImpl) GetMetadata(functionName string, functionVersion string) (*model.FunctionMetadata, error) {
	if functionVersion == "latest" {
		bundle, err := f.resolveLatest(functionName, "metadata")
		if err != nil {
			return nil, err
		}
		if bundle != nil {
			return &bundle.Metadata, nil
		}
		panic("This should be unreachable!!!")
	}

	meta, ok := f.MetadataBinaries.Get(functionName, functionVersion)
	if ok {
		return &meta, nil
	}

	fnBundle, err := f.fetchAndStore(functionName, functionVersion)
	if err != nil {
		return nil, err
	}
	return &fnBundle.Metadata, err
}

func (f *FunctionCacheImpl) getSnapshot(functionName string, functionVersion string, want string) (*FunctionBundle, error) {
	switch want {
	case "metadata":
		if meta, ok := f.MetadataBinaries.Get(functionName, functionVersion); ok {
			return &FunctionBundle{Metadata: meta}, nil
		}

	case "binary":
		if bin, _, okBin, err := f.MetadataBinaries.GetWithBinary(functionName, functionVersion); okBin && len(bin) > 0 {
			return &FunctionBundle{Binary: bin}, err
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
				return &FunctionBundle{Templates: tmpls}, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown want type: %s", want)
	}
	return nil, nil
}

func (f *FunctionCacheImpl) resolveLatest(functionName, want string) (*FunctionBundle, error) {
	cachedVersion, found, expired := f.LatestVersions.GetWithStatus(functionName)
	bundle, err := f.getSnapshot(functionName, cachedVersion, want)
	if err != nil {
		return nil, err
	}

	if bundle == nil {
		fetchedBundle, err := f.fetcher.Fetch(functionName, "latest")
		if err != nil {
			return nil, err
		}

		f.LatestVersions.Set(functionName, fetchedBundle.Metadata.Version)
		err = f.store(fetchedBundle)
		if err != nil {
			return nil, err
		}
		return fetchedBundle, nil
	} else {
		if found && !expired {
			return bundle, nil
		}

		fetchedBundle, err := f.fetcher.FetchLatest(functionName, cachedVersion)
		if fetchedBundle != nil {
			f.LatestVersions.Set(functionName, fetchedBundle.Metadata.Version)
			err = f.store(fetchedBundle)
			if err != nil {
				return nil, err
			}
			return fetchedBundle, nil
		}

		// refresh latest cache
		f.LatestVersions.Set(functionName, cachedVersion)
		return bundle, err
	}
}

func (f *FunctionCacheImpl) store(bundle *FunctionBundle) error {
	for name, template := range bundle.Templates {
		f.Templates.Set(name, template)
	}
	if err := f.MetadataBinaries.Set(bundle.Metadata, bundle.Binary); err != nil {
		return err
	}
	return nil
}

func (f *FunctionCacheImpl) fetchAndStore(functionName string, functionVersion string) (*FunctionBundle, error) {
	if functionVersion == "latest" {
		panic("This should be unreachable!!!")
	}

	key := functionName + "@" + functionVersion
	fetchedValue, err, _ := f.group.Do(key, func() (any, error) {

		bundle, err := f.fetcher.Fetch(functionName, functionVersion)
		if err != nil {
			return nil, err
		}

		if bundle == nil || len(bundle.Binary) == 0 || bundle.Templates == nil {
			return nil, fmt.Errorf("invalid bundle from registry")
		}

		err = f.store(bundle)
		if err != nil {
			return nil, err
		}

		return bundle, nil
	})

	if err != nil {
		return nil, err
	}
	return fetchedValue.(*FunctionBundle), nil
}
