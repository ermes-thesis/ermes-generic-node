package caches

import (
	"config"
	"os"
	"path/filepath"
)

type BinaryCache struct {
	cache  *BaseCache[string]
	binDir string
}

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

func (c *BinaryCache) Set(functionName string, binary []byte) error {
	binPath := filepath.Join(c.binDir, functionName)
	err := os.WriteFile(binPath, binary, 0644)
	if err != nil {
		return err
	}

	c.cache.Set(functionName, binPath)
	return nil
}

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
