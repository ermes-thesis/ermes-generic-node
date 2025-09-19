package test_utils

import (
	"config"
	"context"
	"db/redis"
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func GetDockerHost() string {
	addrs, err := net.LookupHost("host.docker.internal")
	if err == nil && len(addrs) > 0 {
		return "host.docker.internal"
	}

	return "172.17.0.1"
}

func StartRedisTestContainer(t *testing.T) (*redis.RedisClient, func()) {
	t.Setenv("TESTCONTAINERS_HOST_OVERRIDE", GetDockerHost())
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

	//https://redis.io/docs/latest/develop/ai/search-and-query/indexing/#index-limitations:~:text=After%20you%20create,modify%20command%20finishes
	err = client.CreateIndex()
	assert.NoError(t, err, "Expected no error when creating index")

	return client, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
