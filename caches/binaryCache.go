package caches

import (
	"config"
	"os"
	"path/filepath"
)

// BinaryCache stores compiled binaries on disk and keeps track of them
// using an underlying BaseCache. Evicted binaries are automatically removed
// from disk.
type BinaryCache struct {
	cache  *BaseCache[string]
	binDir string
}

// NewBinaryCache creates a new BinaryCache based on the given configuration.
// Evicted binaries are automatically deleted from disk.
//
// Parameters:
//   - config: config.BinaryCacheConfig containing the cache size, TTL, and binary directory
//
// Returns:
//   - *BinaryCache: the newly created binary cache
//   - error: non-nil if the underlying BaseCache could not be created
func NewBinaryCache(config config.BinaryCacheConfig) (*BinaryCache, error) {
	binCache, err := NewBaseCacheWithEviction(config.Size, config.Ttl, func(key string, item CacheItem[string]) {
		_ = os.Remove(item.Value)
	})
	if err != nil {
		return nil, err
	}

	return &BinaryCache{
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
func (c *BinaryCache) Set(functionName string, binary []byte) error {
	binPath := filepath.Join(c.binDir, functionName)
	err := os.WriteFile(binPath, binary, 0644)
	if err != nil {
		return err
	}

	c.cache.Set(functionName, binPath)
	return nil
}

// Get retrieves the binary associated with the given function name from disk.
//
// Parameters:
//   - functionName: the name of the function to retrieve
//
// Returns:
//   - []byte: the binary content (nil if not found)
//   - bool: true if the binary was found, false otherwise
//   - error: non-nil if reading the file from disk fails
func (c *BinaryCache) Get(functionName string) ([]byte, bool, error) {
	if binPath, ok := c.cache.Get(functionName); ok {
		bin, err := os.ReadFile(binPath)
		if err != nil {
			return nil, false, err
		}
		return bin, true, nil
	}
	return nil, false, nil
}
