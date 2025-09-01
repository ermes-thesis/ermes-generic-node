package caches

import (
	"config"
	"github.com/stretchr/testify/require"
	"metadataManager/utils"
	"testing"
	"time"
)

func TestTemplatesCache_SetAndGet(t *testing.T) {
	cfg := config.TemplatesCacheConfig{
		Size: 10,
		Ttl:  1 * time.Second,
	}

	cache, err := NewTemplatesCache(cfg)
	require.NoError(t, err)

	fn := "func1"
	templates := map[string]utils.Query{
		"tmpl1": {
			Select: "ALL",
			As:     "test1",
			Where:  nil,
		},
		"tmpl2": {
			Select: "ALL",
			As:     "test2",
			Where:  nil,
		},
	}

	cache.Set(fn, templates)

	templs, ok := cache.Get(fn)
	require.True(t, ok)

	require.Len(t, templs, len(templates))

	require.Equal(t, templates["tmpl1"], templs["tmpl1"])
	require.Equal(t, templates["tmpl2"], templs["tmpl2"])
}

func TestTemplatesCache_GetMissing(t *testing.T) {
	cfg := config.TemplatesCacheConfig{
		Size: 10,
		Ttl:  1 * time.Second,
	}

	cache, err := NewTemplatesCache(cfg)
	require.NoError(t, err)

	templs, ok := cache.Get("missing")
	require.False(t, ok)
	require.Zero(t, templs)
}

func TestTemplatesCache_TTLExpiry(t *testing.T) {
	cfg := config.TemplatesCacheConfig{
		Size: 10,
		Ttl:  50 * time.Millisecond,
	}

	cache, err := NewTemplatesCache(cfg)
	require.NoError(t, err)

	fn := "func1"
	templates := map[string]utils.Query{
		"tmpl": {
			Select: "ALL",
			As:     "test",
			Where:  nil,
		},
	}
	cache.Set(fn, templates)

	// Immediately should exist
	templs, ok := cache.Get(fn)
	require.True(t, ok)
	require.Equal(t, templates["tmpl"], templs["tmpl"])

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	templs, ok = cache.Get(fn)
	require.False(t, ok)
	require.Zero(t, templs)
}
