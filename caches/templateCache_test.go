package caches

import (
	"config"
	"metadataManager/utils"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTemplatesCache_SetAndGet(t *testing.T) {
	cfg := config.TemplatesCacheConfig{
		Size: 10,
		Ttl:  1 * time.Second,
	}

	cache, err := NewTemplatesCache(cfg)
	require.NoError(t, err)

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

	cache.Set("tmpl1", templates["tmpl1"])
	cache.Set("tmpl2", templates["tmpl2"])

	q1, ok := cache.Get("tmpl1")
	require.True(t, ok)
	require.Equal(t, q1, templates["tmpl1"])
	q2, ok := cache.Get("tmpl2")
	require.True(t, ok)
	require.Equal(t, q2, templates["tmpl2"])
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
	cache.Set(fn, templates["tmpl"])

	// Immediately should exist
	q, ok := cache.Get(fn)
	require.True(t, ok)
	require.Equal(t, q, templates["tmpl"])

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	qLater, ok := cache.Get(fn)
	require.False(t, ok)
	require.Zero(t, qLater)
}
