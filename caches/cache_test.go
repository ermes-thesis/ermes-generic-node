package caches

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBaseCache_SetAndGet(t *testing.T) {
	cache, err := NewBaseCache[string](10, 5*time.Second)
	require.NoError(t, err)

	cache.Set("foo", "bar")

	val, ok := cache.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", val)
}

func TestBaseCache_GetExpired(t *testing.T) {
	cache, err := NewBaseCache[string](10, 10*time.Millisecond)
	require.NoError(t, err)

	cache.Set("foo", "bar")

	time.Sleep(20 * time.Millisecond)

	v, ok := cache.Get("foo")
	require.False(t, ok)
	require.Zero(t, v)
}

func TestBaseCacheWithEviction(t *testing.T) {
	// Making a channel to test if the evictionFunction is actually executed
	evicted := make(chan string, 1)

	// Eviction function is called after a key is removed from the cache, and it sends the removed key to the channel
	// i.e. if we found the just removed key in the channel the eviction function has been executed
	evictFn := func(key string, item CacheItem[string]) {
		evicted <- key
	}

	cache, err := NewBaseCacheWithEviction[string](1, 0, evictFn)
	require.NoError(t, err)

	cache.Set("foo", "bar")
	cache.Set("baz", "qux") // Since cache size is 1, the "foo" key should be evicted

	select {
	case key := <-evicted:
		require.Equal(t, "foo", key)
	case <-time.After(1 * time.Second):
		require.Fail(t, "timed out waiting for eviction")
	}
}

func TestBaseCache_GetMissing(t *testing.T) {
	cache, err := NewBaseCache[string](10, 0)
	require.NoError(t, err)

	v, ok := cache.Get("missing")
	require.False(t, ok)
	require.Zero(t, v)
}
