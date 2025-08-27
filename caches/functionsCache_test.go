package caches

import (
	caches_mock "caches/mocks"
	"config"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFunctionCacheImpl_GetTemplateAndBinary(t *testing.T) {
	tmpDir := t.TempDir()

	fn := "func1"

	fetcher := &caches_mock.Fetcher[*FunctionBundle]{}
	fetcher.EXPECT().Fetch(fn).Return(&FunctionBundle{
		Binary:    []byte("binary-" + fn),
		Templates: map[string]string{"tmpl" + fn: "value-" + fn},
	}, nil)

	cfg := config.FunctionsCacheConfig{
		TemplateCache: config.TemplatesCacheConfig{
			Size: 10,
			Ttl:  1 * time.Second,
		},
		BinaryCache: config.BinaryCacheConfig{
			Size:   10,
			Ttl:    1 * time.Second,
			BinDir: tmpDir,
		},
	}

	cache, err := NewFunctionCache(cfg, fetcher)
	require.NoError(t, err)

	// Initially cache is empty, so fetcher should be called
	tmpl, err := cache.GetTemplate(fn)
	require.NoError(t, err)
	require.Equal(t, "value-"+fn, tmpl["tmpl"+fn])
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)

	// Requesting binary for the same fn. Fetcher SHOULD NOT BE CALLED
	bin, err := cache.GetBinary(fn)
	require.NoError(t, err)
	require.Equal(t, []byte("binary-"+fn), bin)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)

	// Second calls should hit cache, fetcher not called again
	tmpl2, err := cache.GetTemplate(fn)
	require.NoError(t, err)
	bin2, err := cache.GetBinary(fn)
	require.NoError(t, err)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)

	require.Equal(t, tmpl, tmpl2)
	require.Equal(t, bin, bin2)

	// Check that binary file exists on disk
	binPath := filepath.Join(tmpDir, fn)
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		require.Fail(t, fmt.Sprintf("%s does not exist", binPath))
	}
}

func TestFunctionCacheImpl_FetcherReturnsInvalidBundle(t *testing.T) {
	tmpDir := t.TempDir()

	fnName := "func1"
	fetcher := &caches_mock.Fetcher[*FunctionBundle]{}
	// Return invalid bundle (empty binary)
	fetcher.EXPECT().Fetch(fnName).Return(&FunctionBundle{
		Binary:    []byte{},
		Templates: map[string]string{"tmpl": "value"},
	}, nil)

	cfg := config.FunctionsCacheConfig{
		TemplateCache: config.TemplatesCacheConfig{
			Size: 10,
			Ttl:  1 * time.Second,
		},
		BinaryCache: config.BinaryCacheConfig{
			Size:   10,
			Ttl:    1 * time.Second,
			BinDir: tmpDir,
		},
	}

	cache, err := NewFunctionCache(cfg, fetcher)
	require.NoError(t, err)

	_, err = cache.GetBinary("func1")
	require.Error(t, err)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)
	_, err = cache.GetTemplate("func1")
	require.Error(t, err)
	fetcher.AssertNumberOfCalls(t, "Fetch", 2)
}
