package metadataManager_test

import (
	"bytes"
	caches_mock "caches/mocks"
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
	"metadataManager/utils"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"server"
	"server/info"
	"strings"
	"testing"
	"time"
	tokenDecoder_mock "tokenDecoder/mocks"
)

var templates = map[string]utils.Query{
	"private_data_query": {
		Select: "ALL",
		As:     "private_data",
		Where: &utils.ConditionWrapper{Condition: &utils.AndCondition{
			Conditions: []utils.Condition{
				&utils.EqCondition{
					Field: "owner",
					Value: "12345",
				},
				&utils.EqCondition{
					Field: "name",
					Value: "<name>",
				},
			},
		}},
	},
	"gt_data_query": {
		Select: "ALL",
		As:     "gt_data",
		Where: &utils.ConditionWrapper{Condition: &utils.AndCondition{
			Conditions: []utils.Condition{
				&utils.EqCondition{
					Field: "owner",
					Value: 12345,
				},
				&utils.EqCondition{
					Field: "name",
					Value: "DATI_PRIVATE",
				},
				&utils.GtCondition{
					Field: "created_at",
					Value: 1745747999,
				},
			},
		}},
	},
}

//var templates = map[string]string{
//	"private_data_query": `
//			{
//			  "SELECT": "ALL",
//			  "AS": "private_data",
//			  "WHERE": {
//				"AND": [
//				  {
//					"field": "owner",
//					"eq": "12345"
//				  },
//				  {
//					"field": "name",
//					"eq": "<name>"
//				  }
//				]
//			  }
//			}
//	`,
//	"gt_data_query": `
//			{
//			  "SELECT": "ALL",
//			  "AS": "gt_data",
//			  "WHERE": {
//				"AND": [
//				  {
//					"field": "owner",
//					"eq": "12345"
//				  },
//				  {
//					"field": "name",
//					"eq": "DATI_PRIVATE"
//				  },
//				  {
//					"field": "created_at",
//					"gt": 1745747999
//				  }
//				]
//			  }
//			}
//	`,
//}

func TestMetadataLookupIntegration(t *testing.T) {
	redisClient1, shutdownFunc1 := startRedisContainer(t, false)
	defer shutdownFunc1()
	defer redisClient1.Close()

	redisClient2, shutdownFunc2 := startRedisContainer(t, true)
	defer shutdownFunc2()
	defer redisClient2.Close()

	serverInfoFather := &info.ServerInfo{
		Father:    nil,
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Secret: "secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	tokenString := "JWTToken"

	fnCache := &caches_mock.FunctionCache{}
	q := templates["private_data_query"]
	q2 := templates["gt_data_query"]
	fnCache.EXPECT().GetTemplate(invokedFnName, invokedFnVer, "private_data_query").Return(&q, nil)
	fnCache.EXPECT().GetTemplate(invokedFnName, invokedFnVer, "gt_data_query").Return(&q2, nil)

	tknDecoder := &tokenDecoder_mock.TokenDecoder{}
	tknDecoder.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClient1, serverInfoFather, os.Stdout, fnCache, tknDecoder)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient1, metadataManagerFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			Address: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret: "secret",
	}
	fnCache2 := &caches_mock.FunctionCache{}

	tknDecoder2 := &tokenDecoder_mock.TokenDecoder{}
	tknDecoder2.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClient2, serverInfoChild, os.Stdout, fnCache2, tknDecoder2)

	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient2, metadataManagerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []info.Node{
		{
			Address: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/lookup", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
	})
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-QUERY", string(queryParams))
	req.Header.Set("X-ERMES-GROUPS", tokenString)
	// These headers are needed only for testing purposes. Since we don't have a way to simulate a function
	// invocation request, we have to pass manually the invoked function name and version as headers to make the
	// metadataManager work properly.
	req.Header.Set("X-ERMES-REQ-FUNCTION", invokedFnName)
	req.Header.Set("X-ERMES-REQ-FUNCTION-VERSION", invokedFnVer)

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

func TestMetadataCreationFromChildIntegration(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisContainer(t, false)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientChild, shutdownFuncChild := startRedisContainer(t, true)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father:    nil,
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Secret: "secret",
	}
	tokenString := "JWTToken"

	fnCache := &caches_mock.FunctionCache{}
	tknDecoder := &tokenDecoder_mock.TokenDecoder{}
	tknDecoder.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, os.Stdout, fnCache, tknDecoder)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClientFather, metadataManagerFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			Address:        strings.Split(httpServerFather.URL, ":")[2],
			MetadataDomain: "IT",
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret: "secret",
	}
	fnCacheChild := &caches_mock.FunctionCache{}
	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, os.Stdout, fnCacheChild, tknDecoderChild)

	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClientChild, metadataManagerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []info.Node{
		{
			Address:        strings.Split(httpServerChild.URL, ":")[2],
			MetadataDomain: "IT/Milan",
		},
	}

	metadata := db.Metadata{
		Name:           "test",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    db.EVENTUAL_CONSISTENT,
		Geolocation:    "IT/Milan",
		CustomMetadata: map[string]string{},
	}
	jsonMetadata, err := json.Marshal(metadata)
	assert.NoError(t, err, "Expected no error when marshalling metadata")

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/create_metadata/12345678", bytes.NewBuffer(jsonMetadata))
	assert.NoError(t, err, "Expected no error when creating request")
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "IT/Milan/test")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	res.Body.Close()

	assert.Equal(t, http.StatusCreated, res.StatusCode, "Expected status code 200 OK")

	bodyResp, err := io.ReadAll(res.Body)
	assert.NoError(t, err, "Expected no error when reading response body")
	assert.Empty(t, bodyResp, "Expected empty response body")

	// Verify that the metadata was created in both father and child
	metaChild, err := redisClientChild.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata, *metaChild, "Expected metadata to match")

	metaFather, err := redisClientFather.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Expected metadata to match")
}

func TestMetadataCreationFromFatherIntegration(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisContainer(t, false)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientChild, shutdownFuncChild := startRedisContainer(t, true)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father: &info.Node{
			//root node, so that the father can think that the message arrive from top
		},
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Secret: "secret",
	}
	tokenString := "JWTToken"

	fnCache := &caches_mock.FunctionCache{}
	tknDecoderFather := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, os.Stdout, fnCache, tknDecoderFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClientFather, metadataManagerFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			Address:        strings.Split(httpServerFather.URL, ":")[2],
			MetadataDomain: "IT",
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret: "secret",
	}
	fnCacheChild := &caches_mock.FunctionCache{}
	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, os.Stdout, fnCacheChild, tknDecoderChild)

	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClientChild, metadataManagerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []info.Node{
		{
			Address:        strings.Split(httpServerChild.URL, ":")[2],
			MetadataDomain: "IT/Milan",
		},
	}

	metadata := db.Metadata{
		Name:           "test",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    db.EVENTUAL_CONSISTENT,
		Geolocation:    "IT/Milan",
		CustomMetadata: map[string]string{},
	}
	jsonMetadata, err := json.Marshal(metadata)
	assert.NoError(t, err, "Expected no error when marshalling metadata")

	req, err := http.NewRequest(http.MethodPost, httpServerFather.URL+"/create_metadata/12345678", bytes.NewBuffer(jsonMetadata))
	assert.NoError(t, err, "Expected no error when creating request")
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	res.Body.Close()

	assert.Equal(t, http.StatusCreated, res.StatusCode, "Expected status code 200 OK")

	bodyResp, err := io.ReadAll(res.Body)
	assert.NoError(t, err, "Expected no error when reading response body")
	assert.Empty(t, bodyResp, "Expected empty response body")

	// Verify that the metadata was created in both father and child
	metaChild, err := redisClientChild.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata, *metaChild, "Expected metadata to match")

	metaFather, err := redisClientFather.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Expected metadata to match")
}

func TestMetadataCreationFromLocalIntegration(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisContainer(t, false)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientLocal, shutdownFuncLocal := startRedisContainer(t, true)
	defer shutdownFuncLocal()
	defer redisClientLocal.Close()

	redisClientChild, shutdownFuncChild := startRedisContainer(t, true)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father:    nil,
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Children: []info.Node{
			{
				MetadataDomain: "IT/Milan",
			},
		},
		Secret: "secret",
	}
	tokenString := "JWTToken"

	fnCacheFather := &caches_mock.FunctionCache{}
	tknDecoderFather := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, os.Stdout, fnCacheFather, tknDecoderFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClientFather, metadataManagerFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			MetadataDomain: "IT/Milan",
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan/Polimi",
		},
		Secret: "secret",
	}
	fnCacheChild := &caches_mock.FunctionCache{}
	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, os.Stdout, fnCacheChild, tknDecoderChild)

	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClientChild, metadataManagerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoLocal := &info.ServerInfo{
		Father: &info.Node{
			Address:        strings.Split(httpServerFather.URL, ":")[2],
			MetadataDomain: "IT",
		},
		Children: []info.Node{
			{
				Address:        strings.Split(httpServerChild.URL, ":")[2],
				MetadataDomain: "IT/Milan/Polimi",
			},
		},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret: "secret",
	}
	fnCacheLocal := &caches_mock.FunctionCache{}
	tknDecoderLocal := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderLocal.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, nil)
	metadataManagerLocal := metadataManager.NewMetadataManager(redisClientLocal, serverInfoLocal, os.Stdout, fnCacheLocal, tknDecoderLocal)

	metadata := db.Metadata{
		Name:           "test",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    db.EVENTUAL_CONSISTENT,
		Geolocation:    "IT/Milan",
		CustomMetadata: map[string]string{},
	}

	err = metadataManagerLocal.CreateMetadata("12345678", metadata, metadataManager.FROM_LOCAL)
	assert.NoError(t, err, "Expected no error when creating metadata from local")

	// Verify that the metadata was created in both father and child
	metaChild, err := redisClientChild.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Nil(t, metaChild, "Expected metadata to be nil in child")

	metaFather, err := redisClientFather.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Expected metadata to match")

	metaLocal, err := redisClientLocal.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from local")
	assert.Equal(t, metadata, *metaLocal, "Expected metadata to match")

	metadata2 := db.Metadata{
		Name:           "test2",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    db.EVENTUAL_CONSISTENT,
		Geolocation:    "IT/Milan/Polimi",
		CustomMetadata: map[string]string{},
	}

	err = metadataManagerLocal.CreateMetadata("test2", metadata2, metadataManager.FROM_LOCAL)
	assert.NoError(t, err, "Expected no error when creating metadata from local")

	// Verify that the metadata was created in both father and child
	metaChild2, err := redisClientChild.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata2, *metaChild2, "Expected metadata to match")

	metaFather2, err := redisClientFather.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata2, *metaFather2, "Expected metadata to match")

	metaLocal2, err := redisClientLocal.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from local")
	assert.Equal(t, metadata2, *metaLocal2, "Expected metadata to match")
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
