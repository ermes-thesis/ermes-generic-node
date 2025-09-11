package caches

import (
	"caches/metadata"
	caches_mock "caches/mocks"
	"config"
	"fmt"
	"metadataManager/utils"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFunctionCacheImpl_GetTemplateAndBinary(t *testing.T) {
	tmpDir := t.TempDir()

	fn := "func1"
	fnVer := "1.0.0"

	fetcher := &caches_mock.Fetcher[*FunctionBundle]{}
	query := utils.Query{
		Select: "ALL",
		As:     "test",
		Where: &utils.ConditionWrapper{Condition: &utils.EqCondition{
			Field: "name",
			Value: "test",
		},
		},
	}
	fetcher.EXPECT().Fetch(fn, fnVer).Return(&FunctionBundle{
		Binary: []byte("binary-" + fn),
		Templates: map[string]utils.Query{
			"tmpl" + fn: query,
		},
		Metadata: metadata.FunctionMetadata{
			Name:                  fn,
			Version:               fnVer,
			TemplateNames:         []string{"tmpl" + fn},
			DeployedOn:            nil,
			StrongCollWriteAccess: false,
		},
	}, nil)

	cfg := config.FunctionsCacheConfig{
		TemplateCache: config.TemplatesCacheConfig{
			Size: 10,
			Ttl:  30 * time.Second,
		},
		BinaryCache: config.MetadataBinaryCacheConfig{
			Size:   10,
			Ttl:    30 * time.Second,
			BinDir: tmpDir,
		},
	}

	cache, err := NewFunctionCache(cfg, fetcher)
	require.NoError(t, err)

	// Initially cache is empty, so fetcher should be called
	q, err := cache.GetTemplate(fn, fnVer, "tmpl"+fn)
	require.NoError(t, err)
	require.Equal(t, query, *q)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)

	// Requesting binary for the same fn. Fetcher SHOULD NOT BE CALLED
	bin, err := cache.GetBinary(fn, fnVer)
	require.NoError(t, err)
	require.Equal(t, []byte("binary-"+fn), bin)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)

	// Second calls should hit cache, fetcher not called again
	q2, err := cache.GetTemplate(fn, fnVer, "tmpl"+fn)
	require.NoError(t, err)
	bin2, err := cache.GetBinary(fn, fnVer)
	require.NoError(t, err)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)

	require.Equal(t, *q, *q2)
	require.Equal(t, bin, bin2)

	// Check that binary file exists on disk
	binPath := filepath.Join(tmpDir, fn+"@"+fnVer)
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		require.Fail(t, fmt.Sprintf("%s does not exist", binPath))
	}
}

func TestFunctionCacheImpl_FetcherReturnsInvalidBundle(t *testing.T) {
	tmpDir := t.TempDir()

	fnName := "func1"
	fnVer := "1.0.0"
	fetcher := &caches_mock.Fetcher[*FunctionBundle]{}
	// Return invalid bundle (empty binary)
	query := utils.Query{
		Select: "ALL",
		As:     "test",
		Where: &utils.ConditionWrapper{Condition: &utils.EqCondition{
			Field: "name",
			Value: "test",
		},
		},
	}
	fetcher.EXPECT().Fetch(fnName, fnVer).Return(&FunctionBundle{
		Binary: []byte{},
		Templates: map[string]utils.Query{
			"tmpl" + fnName: query,
		},
	}, nil)

	cfg := config.FunctionsCacheConfig{
		TemplateCache: config.TemplatesCacheConfig{
			Size: 10,
			Ttl:  1 * time.Second,
		},
		BinaryCache: config.MetadataBinaryCacheConfig{
			Size:   10,
			Ttl:    1 * time.Second,
			BinDir: tmpDir,
		},
	}

	cache, err := NewFunctionCache(cfg, fetcher)
	require.NoError(t, err)

	_, err = cache.GetBinary(fnName, fnVer)
	require.Error(t, err)
	fetcher.AssertNumberOfCalls(t, "Fetch", 1)
	_, err = cache.GetTemplate(fnName, fnVer, "tmpl")
	require.Error(t, err)
	fetcher.AssertNumberOfCalls(t, "Fetch", 2)
}
