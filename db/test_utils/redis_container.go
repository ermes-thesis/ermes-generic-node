package test_utils

import (
	"config"
	"context"
	"db/redis"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// IsWSL returns true if the Go code is running inside WSL2.
func IsWSL() bool {
	if runtime.GOOS != "linux" {
		return false
	}

	// Check /proc/version
	if content, err := os.ReadFile("/proc/version"); err == nil {
		if strings.Contains(strings.ToLower(string(content)), "microsoft") {
			return true
		}
	}

	// Optionally check /proc/sys/kernel/osrelease (some WSL versions)
	if content, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		if strings.Contains(strings.ToLower(string(content)), "microsoft") {
			return true
		}
	}

	return false
}

func StartRedisTestContainer(t *testing.T) (*redis.RedisClient, func()) {
	if IsWSL() {
		t.Setenv("TESTCONTAINERS_HOST_OVERRIDE", "localhost")
	}
	ctx := context.Background()
	redisContainer, err := tcredis.Run(ctx,
		"redis/redis-stack:latest",
		tcredis.WithSnapshotting(10, 1),
		tcredis.WithLogLevel(tcredis.LogLevelVerbose),
		testcontainers.WithCmd("redis-stack-server"),
		testcontainers.WithCmdArgs("--maxmemory", "500mb"),
		//tcredis.WithConfigFile(filepath.Join("testdata", "redis7.conf")),
	)

	if err != nil {
		t.Fatalf("failed to start container: %s", err)
		return nil, nil
	}

	connectionString, _ := redisContainer.ConnectionString(ctx)
	parsedUrl, _ := url.Parse(connectionString)

	client := redis.NewRedisClient(config.RedisConfig{
		Address:  parsedUrl.Hostname(),
		Port:     parsedUrl.Port(),
		Password: "",
		DB:       0,
		Protocol: 2,
	}, ctx)

	return client, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
