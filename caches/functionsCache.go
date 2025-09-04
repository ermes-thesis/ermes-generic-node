package caches

import (
	"caches/metadata"
	"config"
	"fmt"
	"golang.org/x/sync/singleflight"
	"metadataManager/utils"
	"slices"
)

type FunctionCache interface {
	GetTemplate(functionName string, functionVersion string, templateName string) (*utils.Query, error)
	GetBinary(functionName string, functionVersion string) ([]byte, error)
	GetMetadata(functionName string, functionVersion string) (*metadata.FunctionMetadata, error)
}

type FunctionCacheImpl struct {
	Templates        *TemplatesCache
	MetadataBinaries *MetadataBinaryCache
	fetcher          Fetcher[*FunctionBundle]
	group            singleflight.Group
}

func NewFunctionCache(config config.FunctionsCacheConfig, fetcher Fetcher[*FunctionBundle]) (*FunctionCacheImpl, error) {
	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := NewBinaryCache(config.BinaryCache)
	if err != nil {
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}

	return &FunctionCacheImpl{
		Templates:        tmplCache,
		MetadataBinaries: binCache,
		fetcher:          fetcher,
	}, nil
}

func NewDefaultFunctionCache(config config.FunctionsCacheConfig) (*FunctionCacheImpl, error) {
	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := NewBinaryCache(config.BinaryCache)
	if err != nil {
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}

	return &FunctionCacheImpl{
		Templates:        tmplCache,
		MetadataBinaries: binCache,
		fetcher:          NewRemoteBundleFetcher(fmt.Sprintf("http://%s:%s", config.FunctionRegistry.Address, config.FunctionRegistry.Port)),
		group:            singleflight.Group{},
	}, nil
}

func (f *FunctionCacheImpl) GetTemplate(functionName string, functionVersion string, templateName string) (*utils.Query, error) {
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

func (f *FunctionCacheImpl) GetMetadata(functionName string, functionVersion string) (*metadata.FunctionMetadata, error) {
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

func (f *FunctionCacheImpl) fetchAndStore(functionName string, functionVersion string) (*FunctionBundle, error) {
	fetchedValue, err, _ := f.group.Do(functionName, func() (any, error) {
		bundle, err := f.fetcher.Fetch(functionName + "@" + functionVersion)
		if err != nil {
			return nil, err
		}
		if bundle == nil || len(bundle.Binary) == 0 || bundle.Templates == nil {
			return nil, fmt.Errorf("invalid bundle from registry")
		}

		if err = f.MetadataBinaries.Set(bundle.Metadata, bundle.Binary); err != nil {
			return nil, err
		}
		for templateName, q := range bundle.Templates {
			f.Templates.Set(templateName, q)
		}

		return bundle, nil
	})

	if err != nil {
		return nil, err
	}
	return fetchedValue.(*FunctionBundle), nil
}
