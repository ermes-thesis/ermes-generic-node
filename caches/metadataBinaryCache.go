package caches

import (
	"caches/metadata"
	"config"
	"os"
	"path/filepath"
)

// MetadataBinaryCache stores compiled binaries on disk and keeps track of them
// using an underlying BaseCache. Evicted binaries are automatically removed
// from disk.
type MetadataBinaryCache struct {
	cache  *BaseCache[metadata.FunctionMetadata]
	binDir string
}

// NewBinaryCache creates a new MetadataBinaryCache based on the given configuration.
// Evicted binaries are automatically deleted from disk.
//
// Parameters:
//   - config: config.BinaryCacheConfig containing the cache size, TTL, and binary directory
//
// Returns:
//   - *MetadataBinaryCache: the newly created binary cache
//   - error: non-nil if the underlying BaseCache could not be created
func NewBinaryCache(config config.BinaryCacheConfig) (*MetadataBinaryCache, error) {
	binCache, err := NewBaseCacheWithEviction(config.Size, config.Ttl, func(key string, item CacheItem[metadata.FunctionMetadata]) {
		_ = os.Remove(filepath.Join(config.BinDir, item.Value.Name+"@"+item.Value.Version))
	})
	if err != nil {
		return nil, err
	}

	return &MetadataBinaryCache{
		cache:  binCache,
		binDir: config.BinDir,
	}, nil
}

// Set stores a binary on disk and records its path in the cache.
//
// Parameters:
//   - functionName: the name of the function associated with the binary
//   - binary: the binary content to store
//
// Returns:
//   - error: non-nil if writing the file to disk fails
func (c *MetadataBinaryCache) Set(functionMetadata metadata.FunctionMetadata, binary []byte) error {
	functionNameString := functionMetadata.Name + "@" + functionMetadata.Version
	binPath := filepath.Join(c.binDir, functionNameString)

	err := os.WriteFile(binPath, binary, 0644)
	if err != nil {
		return err
	}

	c.cache.Set(functionNameString, functionMetadata)
	return nil
}

func (c *MetadataBinaryCache) GetWithBinary(functionName string, functionVersion string) ([]byte, *metadata.FunctionMetadata, bool, error) {
	functionNameString := functionName + "@" + functionVersion
	binPath := filepath.Join(c.binDir, functionNameString)
	if fnMetadata, ok := c.cache.Get(functionNameString); ok {
		bin, err := os.ReadFile(binPath)
		if err != nil {
			return nil, nil, false, err
		}
		return bin, &fnMetadata, true, nil
	}
	return nil, nil, false, nil
}

func (c *MetadataBinaryCache) Get(functionName string, functionVersion string) (metadata.FunctionMetadata, bool) {
	functionNameString := functionName + "@" + functionVersion
	return c.cache.Get(functionNameString)
}
