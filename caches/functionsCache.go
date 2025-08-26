package caches

import (
	"config"
	"fmt"
	"golang.org/x/sync/singleflight"
)

type FunctionBundle struct {
	Binary    []byte
	Templates []string
}
type BundleFetcher func(remoteFunctionName string) (*FunctionBundle, error)

type FunctionCache struct {
	Templates *TemplatesCache
	Binaries  *BinaryCache
	fetcher   BundleFetcher
	group     singleflight.Group
}

func NewFunctionCache(config config.FunctionsCacheConfig, fetcher BundleFetcher) (*FunctionCache, error) {
	tmplCache, err := NewTemplatesCache(config.TemplateCache)
	if err != nil {
		return nil, fmt.Errorf("error creating template cache: %v", err)
	}
	binCache, err := NewBinaryCache(config.BinaryCache)
	if err != nil {
		return nil, fmt.Errorf("error creating binary cache: %v", err)
	}

	return &FunctionCache{
		Templates: tmplCache,
		Binaries:  binCache,
		fetcher:   fetcher,
	}, nil
}

func (f *FunctionCache) GetTemplate(functionName string) ([]string, error) {
	if tmpl, ok := f.Templates.Get(functionName); ok {
		return tmpl, nil
	}

	fnBundle, err := f.fetchAndStore(functionName)
	if err != nil {
		return nil, err
	}
	return fnBundle.Templates, nil
}

func (f *FunctionCache) GetBinary(functionName string) ([]byte, error) {
	bin, ok, err := f.Binaries.Get(functionName)
	if err != nil {
		return nil, err
	}
	if ok {
		return bin, nil
	}

	fnBundle, err := f.fetchAndStore(functionName)
	if err != nil {
		return nil, err
	}
	return fnBundle.Binary, err
}

func (f *FunctionCache) fetchAndStore(functionName string) (*FunctionBundle, error) {
	fetchedValue, err, _ := f.group.Do(functionName, func() (any, error) {
		bundle, err := f.fetcher(functionName)
		if err != nil {
			return nil, err
		}
		if bundle == nil || len(bundle.Binary) == 0 || bundle.Templates == nil {
			return nil, fmt.Errorf("invalid bundle from registry")
		}

		if err = f.Binaries.Set(functionName, bundle.Binary); err != nil {
			return nil, err
		}
		f.Templates.Set(functionName, bundle.Templates)

		return bundle, nil
	})

	if err != nil {
		return nil, err
	}
	return fetchedValue.(*FunctionBundle), nil
}
