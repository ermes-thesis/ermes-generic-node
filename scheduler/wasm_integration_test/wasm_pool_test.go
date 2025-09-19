package wasm_integration_test

import (
	"context"
	"db/test_utils"
	"fmt"
	"scheduler"
	"server/info"
	"strings"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestWasmPool(t *testing.T) {
	redisClient, shutdownFn := test_utils.StartRedisTestContainer(t)
	defer shutdownFn()

	client := redisClient.Client()

	client.SAdd(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:groups", "test")
	client.Set(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:battiti", "100", 0)
	client.Set(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:testMGET", "200", 0)
	client.Set(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:testMGET2", "300", 0)

	pool := wasm.NewExecutorPool(10)
	defer pool.Close()

	scheduler.RegisterHostFns(pool, client, &info.ServerInfo{})

	executor := pool.Acquire()
	defer executor.Release()

	resp := executor.Invoke("auth_function", "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string))
	//response, resCode := executor.InvokeFn("function", req, ctx)
	//fmt.Println("Invoke function:", string(response), resCode)

	response := executor.Invoke("provaNuovoNome1.2.2", "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

func BenchmarkWasmPool(b *testing.B) {
	b.Setenv("TESTCONTAINERS_HOST_OVERRIDE", test_utils.GetDockerHost())
	redisContainer, err := tcredis.Run(context.Background(),
		"redis:latest",
		tcredis.WithSnapshotting(10, 1),
		tcredis.WithLogLevel(tcredis.LogLevelVerbose),
		//tcredis.WithConfigFile(filepath.Join("testdata", "redis7.conf")),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(redisContainer); err != nil {
			b.Fatalf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		b.Fatalf("failed to start container: %s", err)
		return
	}

	connectionString, _ := redisContainer.ConnectionString(context.Background())
	client := redis.NewClient(&redis.Options{
		Addr: strings.Split(connectionString, "//")[1],
	})

	client.Set(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:battiti", "100", 0)
	client.Set(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:testMGET", "200", 0)

	pool := wasm.NewExecutorPool(10)
	defer pool.Close()

	scheduler.RegisterHostFns(pool, client, &info.ServerInfo{})

	for b.Loop() {
		client.SAdd(context.Background(), "data:255ef4af-3a49-4382-a90d-6524bdee5ad1:groups", "test")

		executor := pool.Acquire()

		resp := executor.Invoke("auth_function", "authEntry", []byte("token"))
		fmt.Println("Auth function:", resp.HttpCode, resp.Response)

		req, ctx := getTestReqAndCtx(resp.Response.(string))
		response := executor.Invoke("provaNuovoNome1.2.2", "entry", req, ctx)
		fmt.Println("Invoke function:", response.Response, response.HttpCode)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func getTestReqAndCtx(userId string) ([]byte, []byte) {
	req := "{\"prova\": \"ciao\"}"

	timestamp, _ := time.Now().MarshalJSON()
	ctx := "{" +
		"\"userId\": \"" + userId + "\"," +
		"\"groupList\": [\"test\"]," +
		"\"collections\": {" +
		"\"test_collection\": [" +
		"{" +
		"\"CollectionId\": \"255ef4af-3a49-4382-a90d-6524bdee5ad1\"," +
		"\"Metadata\": {" +
		"\"consistency\": 1," +
		"\"name\": \"dati\"," +
		"\"owner\": \"pippo\"," +
		"\"createdAt\": " + string(timestamp) + "" +
		"}" +
		"}" +
		"]" +
		"}" +
		"}"
	return []byte(req), []byte(ctx)
}
