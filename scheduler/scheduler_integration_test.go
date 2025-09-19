package scheduler_test

import (
	"caches/metadata"
	caches_mock "caches/mocks"
	collectionsDiscoverer_mock "dataManager/mocks/collectionsDiscoverer"
	shared "dataManager/shared"
	"db"
	"db/test_utils"
	"encoding/json"
	"io"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"scheduler"
	"server"
	"server/info"
	"strings"
	"testing"
	"time"
	tokenDecoder_mock "tokenDecoder/mocks"

	wasm_mock "github.com/ermes-thesis/ermes-wasm-pool/mocks/wasm-pool"
	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	"github.com/stretchr/testify/assert"
)

func TestScheduleOnChild_ReadStrong(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisFatherContainer(t)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientChild, shutdownFuncChild := startRedisChildContainer(t)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father:    nil,
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]utils.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: utils.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: utils.EventualConsistent,
						Geolocation: "IT/Como",
					},
				},
				{
					Id: "1111",
					Metadata: utils.Metadata{
						Name:        "collectionNameFather",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745749000, 0),
						Groups:      []string{"groupC"},
						Consistency: utils.SequentialConsistent,
						Geolocation: "IT",
					},
				},
			},
		},
	}
	wasmContextJson, err := json.Marshal(wasmContext)
	assert.NoError(t, err, "Expected no error when marshalling wasm context")

	tokenString := "JWTToken"

	fnCacheFather := caches_mock.NewFunctionCache(t)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)
	collChild1111 := shared.NewStrongCollection("1111", 40, false, false)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collChild12345)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111").Return(true, collChild1111)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)

	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, os.Stdout, fnCacheFather)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClientFather, tknDecoderFather, metadataManagerFather, schedulerFather)
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
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&metadata.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: false,
	}, nil)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, os.Stdout, fnCacheChild)

	wasmExecutorChild := wasm_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClientChild, tknDecoderChild, metadataManagerChild, schedulerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []info.Node{
		{
			Address: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-MIDDLEWARE-SECRET", "middleware-secret")
	req.Header.Set("X-ERMES-CACHED-QUERY", string(cachedQueries))
	req.Header.Set("X-ERMES-GROUPS", tokenString)

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected status code 200 OK")
	bodyResp, err := io.ReadAll(res.Body)
	assert.NoError(t, err, "Expected no error when reading response body")

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleOnFather_WriteStrong(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisFatherContainer(t)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientChild, shutdownFuncChild := startRedisChildContainer(t)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father:    nil,
		CloudNode: nil,
		Node: info.Node{
			MetadataDomain: "IT",
		},
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]utils.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: utils.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: utils.EventualConsistent,
						Geolocation: "IT/Como",
					},
				},
				{
					Id: "1111",
					Metadata: utils.Metadata{
						Name:        "collectionNameFather",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745749000, 0),
						Groups:      []string{"groupC"},
						Consistency: utils.SequentialConsistent,
						Geolocation: "IT",
					},
				},
			},
		},
	}
	wasmContextJson, err := json.Marshal(wasmContext)
	assert.NoError(t, err, "Expected no error when marshalling wasm context")

	tokenString := "JWTToken"

	fnCacheFather := caches_mock.NewFunctionCache(t)
	fnCacheFather.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&metadata.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collFather12345 := shared.NewWeakCollection("12345", 40, true)
	collFather1111 := shared.NewStrongCollection("1111", 40, false, true)
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345").Return(false, collFather12345)
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("1111").Return(true, collFather1111)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "user", nil)

	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, os.Stdout, fnCacheFather)

	wasmExecutorFather := wasm_mock.NewExecutor(t)
	wasmExecutorFather.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorFather.EXPECT().Release().Return()

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolFather.EXPECT().Acquire().Return(wasmExecutorFather)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClientFather, tknDecoderFather, metadataManagerFather, schedulerFather)
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
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&metadata.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, os.Stdout, fnCacheChild)

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)
	collChild1111 := shared.NewStrongCollection("1111", 40, false, false)

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing url")
	collChild1111.(*shared.StrongCollection).LeaderId = urlFather.Host

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collChild12345)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111").Return(true, collChild1111)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClientChild, tknDecoderChild, metadataManagerChild, schedulerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing url")
	collFather12345.AddHost(urlChild.Host)
	collFather1111.AddHost(urlChild.Host)
	serverInfoFather.Children = []info.Node{
		{
			Address: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-MIDDLEWARE-SECRET", "middleware-secret")
	req.Header.Set("X-ERMES-CACHED-QUERY", string(cachedQueries))
	req.Header.Set("X-ERMES-GROUPS", tokenString)

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected status code 200 OK")
	bodyResp, err := io.ReadAll(res.Body)
	assert.NoError(t, err, "Expected no error when reading response body")
	assert.Equal(t, "function executed correctly", string(bodyResp), "Expected response body to be 'function executed correctly'")
}

func startRedisChildContainer(t *testing.T) (db.GenericDB, func()) {
	client, shutdownFunc := test_utils.StartRedisTestContainer(t)

	//https://redis.io/docs/latest/develop/ai/search-and-query/indexing/#index-limitations:~:text=After%20you%20create,modify%20command%20finishes
	err := client.CreateIndex()
	assert.NoError(t, err, "Expected no error when creating index")

	err = client.SetMetadata("12345", utils.Metadata{
		Name:        "collectionName",
		Owner:       "12345",
		CreatedAt:   time.Unix(1745748000, 0),
		Groups:      []string{"groupA", "groupB", "groupC"},
		Consistency: utils.EventualConsistent,
		Geolocation: "IT/Como",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 12345")

	err = client.SetCollection("12345", map[string]string{
		"key1": "value1",
		"key2": "value2",
	}, []string{"groupA", "groupB", "groupC"})
	assert.NoError(t, err, "Expected no error when setting collection 12345")

	err = client.SetCollection("1111", map[string]string{
		"key1": "value1",
		"key2": "value2",
	}, []string{"groupC"})
	assert.NoError(t, err, "Expected no error when setting collection 12345")

	return client, shutdownFunc
}

func startRedisFatherContainer(t *testing.T) (db.GenericDB, func()) {
	client, shutdownFunc := test_utils.StartRedisTestContainer(t)

	//https://redis.io/docs/latest/develop/ai/search-and-query/indexing/#index-limitations:~:text=After%20you%20create,modify%20command%20finishes
	err := client.CreateIndex()
	assert.NoError(t, err, "Expected no error when creating index")

	err = client.SetMetadata("12345", utils.Metadata{
		Name:        "collectionName",
		Owner:       "12345",
		CreatedAt:   time.Unix(1745748000, 0),
		Groups:      []string{"groupA", "groupB", "groupC"},
		Consistency: utils.EventualConsistent,
		Geolocation: "IT/Como",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 12345")

	err = client.SetMetadata("1111", utils.Metadata{
		Name:        "collectionNameFather",
		Owner:       "12345",
		CreatedAt:   time.Unix(1745749000, 0),
		Groups:      []string{"groupC"},
		Consistency: utils.SequentialConsistent,
		Geolocation: "IT",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 1111")

	err = client.SetCollection("1111", map[string]string{
		"key1": "value1",
		"key2": "value2",
	}, []string{"groupC"})
	assert.NoError(t, err, "Expected no error when setting collection 12345")

	return client, shutdownFunc
}
