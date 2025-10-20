package test_utils

import (
	"config"
	"context"
	"db/redis"
	"net/url"
	"testing"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// ↓↓↓ UNCOMMENT THIS IF YOU NEED TO EXECUTE THE TESTS ON A CONTAINER ↓↓↓

//// IsWSL returns true if the Go code is running inside WSL2.
//func IsWSL() bool {
//	if runtime.GOOS != "linux" {
//		return false
//	}
//
//	// Check /proc/version
//	if content, err := os.ReadFile("/proc/version"); err == nil {
//		if strings.Contains(strings.ToLower(string(content)), "microsoft") {
//			return true
//		}
//	}
//
//	// Optionally check /proc/sys/kernel/osrelease (some WSL versions)
//	if content, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
//		if strings.Contains(strings.ToLower(string(content)), "microsoft") {
//			return true
//		}
//	}
//
//	return false
//}
//
//func GetDockerHost() string {
//	addrs, err := net.LookupHost("host.docker.internal")
//	if err == nil && len(addrs) > 0 {
//		return "host.docker.internal"
//	}
//
//	return "172.17.0.1"
//}

func createNewLogger(t *testing.T) *logger.Logger {
	log, err := logger.NewLogger(logger.Config{
		Level:     zerolog.DebugLevel,
		Format:    "text",
		AddSource: false,
		Muted:     true,
	})
	require.NoError(t, err)
	return log
}

func StartRedisTestContainer(t *testing.T) (*redis.RedisClient, func()) {
	//if !IsWSL() {
	//	t.Setenv("TESTCONTAINERS_HOST_OVERRIDE", GetDockerHost())
	//}
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

	log := createNewLogger(t)
	client, err := redis.NewRedisClient(config.RedisConfig{
		Address:  parsedUrl.Hostname(),
		Port:     parsedUrl.Port(),
		Password: "",
		DB:       0,
		Protocol: 2,
	}, ctx, log)
	require.NoError(t, err, "failed to create redis client")

	return client, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
