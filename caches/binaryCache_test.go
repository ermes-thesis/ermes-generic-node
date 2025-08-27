package caches

import (
	"config"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBinaryCache_SetAndGet(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.BinaryCacheConfig{
		Size:   10,
		Ttl:    1 * time.Second,
		BinDir: tmpDir,
	}

	cache, err := NewBinaryCache(cfg)
	require.NoError(t, err)

	data := []byte("test-content-binary")
	fn := "func1"

	// Set should write file and cache entry
	err = cache.Set(fn, data)
	require.NoError(t, err)

	// Check file exists
	binPath := filepath.Join(tmpDir, fn)
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		require.Fail(t, "binary does not exist")
	}

	// Get should return the same binary content
	binary, ok, err := cache.Get(fn)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, data, binary)
}

func TestBinaryCache_EvictionRemovesFile(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.BinaryCacheConfig{
		Size:   1, // Size 1, so that with two set the first one is evicted
		Ttl:    0,
		BinDir: tmpDir,
	}

	cache, err := NewBinaryCache(cfg)
	require.NoError(t, err)

	fn1 := "func1"
	fn2 := "func2"

	data1 := []byte("aaa")
	data2 := []byte("bbb")

	err = cache.Set(fn1, data1)
	require.NoError(t, err)
	err = cache.Set(fn2, data2)
	require.NoError(t, err)

	// fn1 should have been evicted, so file removed
	binPath1 := filepath.Join(tmpDir, fn1)
	if _, err := os.Stat(binPath1); !os.IsNotExist(err) {
		require.Fail(t, "binary1 exists but should have been removed")
	}

	// fn2 should still exist
	binPath2 := filepath.Join(tmpDir, fn2)
	if _, err := os.Stat(binPath2); os.IsNotExist(err) {
		require.Fail(t, "binary2 should still exists but was removed")
	}
}

func TestBinaryCache_GetMissing(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.BinaryCacheConfig{
		Size:   10,
		Ttl:    1 * time.Second,
		BinDir: tmpDir,
	}

	cache, err := NewBinaryCache(cfg)
	require.NoError(t, err)

	// Nothing set yet
	binary, ok, err := cache.Get("missing")
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, binary)
}
