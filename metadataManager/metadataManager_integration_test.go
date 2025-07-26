package metadataManager_test

import (
	"config"
	"context"
	"db"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"io"
	"metadataManager"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"server"
	"server/info"
	"strings"
	"testing"
	"time"
)

func TestMetadataManagerIntegration(t *testing.T) {
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
	var response map[string][]db.CollectionInfo
	err = json.Unmarshal(bodyResp, &response)
	assert.NoError(t, err, "Expected valid JSON response, got error: %v", err)

	assert.Len(t, response, 1, "Expected one collection in response")
	assert.Contains(t, response, "private_data", "Expected response to contain 'private_data' key")
	assert.Len(t, response["private_data"], 1, "Expected 'private_data' to have one collection")
	collection := response["private_data"][0]

	assert.Equal(t, "1111", collection.Id, "Expected collection ID to be 1111")
	assert.Equal(t, "12345", collection.Metadata.Owner, "Expected owner to match query parameter")
	assert.Equal(t, "collectionNameFather", collection.Metadata.Name, "Expected name to be 'collectionNameFather'")
	assert.Contains(t, collection.Metadata.Groups, "groupC", "Expected groups to be 'groupC'")

	fmt.Println(response)

}

func startRedisContainer(t *testing.T, child bool) (db.GenericDB, func()) {
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
	parsedUrl, _ := url.Parse(connectionString)

	client := db.NewRedisClient(config.RedisConfig{
		Address:  parsedUrl.Hostname(),
		Port:     parsedUrl.Port(),
		Password: "",
		DB:       0,
		Protocol: 2,
	}, ctx)

	//https://redis.io/docs/latest/develop/ai/search-and-query/indexing/#index-limitations:~:text=After%20you%20create,modify%20command%20finishes
	err = client.CreateIndex()
	assert.NoError(t, err, "Expected no error when creating index")

	err = client.SetMetadata("12345", db.Metadata{
		Name:        "collectionName",
		Owner:       "12345",
		CreatedAt:   time.Unix(1745748000, 0),
		Groups:      []string{"groupA", "groupB", "groupC"},
		Consistency: db.EVENTUAL_CONSISTENT,
		Geolocation: "IT/Como",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 12345")

	err = client.SetMetadata("54321", db.Metadata{
		Name:        "collectionName2",
		Owner:       "12345",
		CreatedAt:   time.Unix(745748000, 0),
		Groups:      []string{"groupC"},
		Consistency: db.EVENTUAL_CONSISTENT,
		Geolocation: "IT/Milan",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 54321")

	if !child {
		err = client.SetMetadata("1111", db.Metadata{
			Name:        "collectionNameFather",
			Owner:       "12345",
			CreatedAt:   time.Unix(1745749000, 0),
			Groups:      []string{"groupC"},
			Consistency: db.EVENTUAL_CONSISTENT,
			Geolocation: "IT",
		})
		assert.NoError(t, err, "Expected no error when setting metadata for collection 1111")
	}

	return client, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
