package caches

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"time"
)

type GenericCache[T any] interface {
	Get(key string) (T, bool)
	Set(key string, value T)
}
type CacheItem[T any] struct {
	Value     T
	Timestamp time.Time
}

type BaseCache[T any] struct {
	cache *lru.Cache[string, CacheItem[T]]
	ttl   time.Duration
}

func NewBaseCache[T any](size int, ttl time.Duration) (*BaseCache[T], error) {
	cache, err := lru.New[string, CacheItem[T]](size)
	if err != nil {
		return nil, err
	}
	return &BaseCache[T]{
		cache: cache,
		ttl:   ttl,
	}, nil
}

func NewBaseCacheWithEviction[T any](size int, ttl time.Duration, evictFn func(string, CacheItem[T])) (*BaseCache[T], error) {
	cache, err := lru.NewWithEvict[string, CacheItem[T]](size, evictFn)
	if err != nil {
		return nil, err
	}

	return &BaseCache[T]{
		cache: cache,
		ttl:   ttl,
	}, nil
}

func (c *BaseCache[T]) Get(key string) (T, bool) {
	if item, ok := c.cache.Get(key); ok {
		if c.ttl == 0 || time.Since(item.Timestamp) < c.ttl {
			return item.Value, true
		}

		c.cache.Remove(key)
	}
	var zero T
	return zero, false
}

func (c *BaseCache[T]) Set(key string, value T) {
	c.cache.Add(key, CacheItem[T]{
		Value:     value,
		Timestamp: time.Now(),
	})
}
