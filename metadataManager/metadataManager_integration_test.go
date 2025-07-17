package metadataManager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"io"
	"metadataManager"
	"net/http"
	"net/http/httptest"
	"os"
	"server"
	"server/info"
	"strings"
	"testing"
)

func TestLookupManagerIntegration(t *testing.T) {
	redisClient1, shutdownFunc1 := startRedisContainer(t, false)
	defer shutdownFunc1()
	defer redisClient1.Close()

	redisClient2, shutdownFunc2 := startRedisContainer(t, true)
	defer shutdownFunc2()
	defer redisClient2.Close()

	serverinfo1 := &info.ServerInfo{
		Father:    nil,
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Secret: "secret",
	}
	metadataManager1 := metadataManager.NewMetadataManager(redisClient1, serverinfo1, os.Stdout)

	ser1, err := server.NewServer(os.Stdout, serverinfo1, redisClient1, metadataManager1)
	assert.NoError(t, err, "Expected no error when creating server")

	srv1 := httptest.NewServer(ser1.Handler)
	defer srv1.Close()

	serverinfo2 := &info.ServerInfo{
		Father: &info.Node{
			Address: strings.Split(srv1.URL, ":")[2],
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret: "secret",
	}
	metadataManager2 := metadataManager.NewMetadataManager(redisClient2, serverinfo2, os.Stdout)

	ser2, err := server.NewServer(os.Stdout, serverinfo2, redisClient2, metadataManager2)
	assert.NoError(t, err, "Expected no error when creating server")

	srv2 := httptest.NewServer(ser2.Handler)
	defer srv2.Close()

	serverinfo1.Children = []info.Node{
		{
			Address: strings.Split(srv2.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, srv2.URL+"/lookup", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
	})
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-QUERY", string(queryParams))

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected status code 200 OK")
	bodyResp, err := io.ReadAll(res.Body)

	fmt.Println("Response: " + string(bodyResp))

	assert.NoError(t, err, "Expected no error when reading response body")
	var response map[string][]metadataManager.CollectionInfo
	err = json.Unmarshal(bodyResp, &response)
	assert.NoError(t, err, "Expected valid JSON response, got error: %v", err)

	assert.Len(t, response, 1, "Expected one collection in response")
	assert.Contains(t, response, "private_data", "Expected response to contain 'private_data' key")
	assert.Len(t, response["private_data"], 1, "Expected 'private_data' to have one collection")
	collection := response["private_data"][0]

	assert.Equal(t, "1111", collection.Id, "Expected collection ID to be 1111")
	assert.NotEmpty(t, collection.Metadata, "Expected collection metadata to be non-empty")
	assert.Contains(t, collection.Metadata, "owner", "Expected metadata to contain 'owner' key")
	assert.Equal(t, "12345", collection.Metadata["owner"], "Expected owner to match query parameter")
	assert.Contains(t, collection.Metadata, "name", "Expected metadata to contain 'name' key")
	assert.Equal(t, "collectionNameFather", collection.Metadata["name"], "Expected name to be 'collectionNameFather'")
	assert.Contains(t, collection.Metadata, "groups", "Expected metadata to contain 'groups' key")
	assert.Equal(t, "groupC", collection.Metadata["groups"], "Expected groups to be 'groupC'")

	fmt.Println(response)

}

func startRedisContainer(t *testing.T, child bool) (*redis.Client, func()) {
	ctx := context.Background()
	redisContainer, err := tcredis.Run(ctx,
		"redis/redis-stack:latest",
		tcredis.WithSnapshotting(10, 1),
		tcredis.WithLogLevel(tcredis.LogLevelVerbose),
		testcontainers.WithCmd("redis-stack-server"),
		//tcredis.WithConfigFile(filepath.Join("testdata", "redis7.conf")),
	)

	if err != nil {
		t.Fatalf("failed to start container: %s", err)
		return nil, nil
	}

	connectionString, _ := redisContainer.ConnectionString(ctx)

	client := redis.NewClient(&redis.Options{
		Addr:     strings.Split(connectionString, "//")[1],
		Protocol: 2,
	})

	client.HSet(ctx, "meta:12345", "name", "collectionName", "owner", "12345", "created_at", 1745748000, "groups", "groupA,groupB,groupC")
	client.HSet(ctx, "meta:54321", "name", "collectionName2", "owner", "12345", "created_at", 1745748000, "groups", "groupC")

	if !child {
		client.HSet(ctx, "meta:1111", "name", "collectionNameFather", "owner", "12345", "created_at", 1745749000, "groups", "groupC")
	}

	client.Set(ctx, "data:12345:record_name", "{\"temperature\":22.5,\"humidity\":55}", 0)
	client.Set(ctx, "data:54321:record_name", "HELLO_WORLD", 0)

	client.SAdd(ctx, "data:12345:groups", "groupA", "groupB", "groupC")
	client.SAdd(ctx, "data:54321:groups", "groupC")

	client.FTCreate(ctx, "metadata_index", &redis.FTCreateOptions{
		OnHash: true,
		Prefix: []any{1, "meta:"},
	}, &redis.FieldSchema{
		FieldName: "name",
		FieldType: redis.SearchFieldTypeText,
	}, &redis.FieldSchema{
		FieldName: "owner",
		FieldType: redis.SearchFieldTypeText,
	}, &redis.FieldSchema{
		FieldName: "created_at",
		FieldType: redis.SearchFieldTypeNumeric,
	}, &redis.FieldSchema{
		FieldName: "groups",
		FieldType: redis.SearchFieldTypeTag,
	})

	return client, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
