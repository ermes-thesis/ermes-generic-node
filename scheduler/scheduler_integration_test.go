package scheduler_test

import (
	"caches/metadata"
	shared "dataManager/shared"
	"encoding/json"
	"io"
	"metadataManager/utils"
	"mocks/caches"
	"mocks/dataManager/collectionsDiscoverer"
	"mocks/db"
	"mocks/ermes-wasm-pool/wasm-pool"
	"mocks/metadataManager"
	"mocks/tokenDecoder"
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

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	"github.com/stretchr/testify/assert"
)

func TestScheduleOnChild_ReadStrong(t *testing.T) {
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

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
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

	wasmExecutorChild := wasm_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
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
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorFather := wasm_mock.NewExecutor(t)
	wasmExecutorFather.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorFather.EXPECT().Release().Return()

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolFather.EXPECT().Acquire().Return(wasmExecutorFather)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
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
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleOnChild_WeakReplicated(t *testing.T) {
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
			},
		},
	}
	wasmContextJson, err := json.Marshal(wasmContext)
	assert.NoError(t, err, "Expected no error when marshalling wasm context")

	tokenString := "JWTToken"

	fnCacheFather := caches_mock.NewFunctionCache(t)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collChild12345)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleOnFather_EmptyChildWeak(t *testing.T) {
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
			},
		},
	}

	wasmContextJson, err := json.Marshal(wasmContext)
	assert.NoError(t, err, "Expected no error when marshalling wasm context")

	tokenString := "JWTToken"

	fnCacheFather := caches_mock.NewFunctionCache(t)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collFather12345 := shared.NewWeakCollection("12345", 40, false)
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collFather12345)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorFather := wasm_mock.NewExecutor(t)
	wasmExecutorFather.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorFather.EXPECT().Release().Return()

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolFather.EXPECT().Acquire().Return(wasmExecutorFather)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			Address: urlFather.Host,
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(false, nil)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleOnChild_DoubleWeak(t *testing.T) {
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
					Id: "54321",
					Metadata: utils.Metadata{
						Name:        "collectionName2",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: utils.EventualConsistent,
						Geolocation: "IT/Como",
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

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)
	collChild54321 := shared.NewWeakCollection("54321", 40, true)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collChild12345)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("54321").Return(false, collChild54321)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleOnChild_EmptyFather(t *testing.T) {
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
			},
		},
	}
	wasmContextJson, err := json.Marshal(wasmContext)
	assert.NoError(t, err, "Expected no error when marshalling wasm context")

	tokenString := "JWTToken"

	fnCacheFather := caches_mock.NewFunctionCache(t)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, true)

	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345").Return(false, collChild12345)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345Child := shared.NewWeakCollection("12345", 40, false)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collChild12345Child)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	collChild12345.AddHost(urlChild.Host)

	req, err := http.NewRequest(http.MethodPost, httpServerFather.URL+"/invoke-function/test_function/1.0.0", nil)
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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleOnChild_WriteStrong(t *testing.T) {
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
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: utils.SequentialConsistent,
						Geolocation: "IT/Como",
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
	collFather12345 := shared.NewWeakCollection("12345", 40, false)
	collFather1111 := shared.NewStrongCollection("1111", 40, false, false)

	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345").Return(true, collFather12345)
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("1111").Return(true, collFather1111)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
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

	wasmExecutorChild := wasm_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild1111 := shared.NewStrongCollection("1111", 40, false, true)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111").Return(true, collChild1111)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345").Return(false, nil)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	collFather1111.AddHost(urlChild.Host)
	collFather1111.(*shared.StrongCollection).LeaderId = urlChild.Host

	req, err := http.NewRequest(http.MethodPost, httpServerFather.URL+"/invoke-function/test_function/1.0.0", nil)
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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}

func TestScheduleThreeNodes(t *testing.T) {
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
					Id: "1111",
					Metadata: utils.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: utils.SequentialConsistent,
						Geolocation: "IT/Como",
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
	collFather1111 := shared.NewStrongCollection("1111", 40, true, false)

	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("1111").Return(false, collFather1111)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	redisClient := db_mock.NewGenericDB(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFather := scheduler.NewScheduler(collectionDiscovererFather, fnCacheFather, wasmExecutorPoolFather, serverInfoFather)

	serverFather, err := server.NewServer(os.Stdout, serverInfoFather, redisClient, tknDecoderFather, metadataManager, schedulerFather)
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
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
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

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild1111 := shared.NewStrongCollection("1111", 40, true, false)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111").Return(false, collChild1111)

	schedulerChild := scheduler.NewScheduler(collectionDiscovererChild, fnCacheChild, wasmExecutorPoolChild, serverInfoChild)
	serverChild, err := server.NewServer(os.Stdout, serverInfoChild, redisClient, tknDecoderChild, metadataManager, schedulerChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoChild2 := &info.ServerInfo{
		Father: &info.Node{
			Address: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children: []info.Node{},
		Node: info.Node{
			MetadataDomain: "IT/Milan",
		},
		Secret:           "secret",
		MiddlewareSecret: "middleware-secret",
	}

	fnCacheChild2 := caches_mock.NewFunctionCache(t)
	fnCacheChild2.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&metadata.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)

	tknDecoderChild2 := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild2.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild2 := wasm_mock.NewExecutor(t)
	wasmExecutorChild2.EXPECT().Invoke(invokedFnName+"@"+invokedFnVer, "entry", [][]byte{{}, wasmContextJson}).Return(wasm.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild2.EXPECT().Release().Return()

	wasmExecutorPoolChild2 := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild2.EXPECT().Acquire().Return(wasmExecutorChild2)

	collectionDiscovererChild2 := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild1111_2 := shared.NewStrongCollection("1111", 40, false, true)

	collectionDiscovererChild2.EXPECT().IsCollectionLocallyPresent("1111").Return(true, collChild1111_2)

	schedulerChild2 := scheduler.NewScheduler(collectionDiscovererChild2, fnCacheChild2, wasmExecutorPoolChild2, serverInfoChild2)
	serverChild2, err := server.NewServer(os.Stdout, serverInfoChild2, redisClient, tknDecoderChild2, metadataManager, schedulerChild2)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild2 := httptest.NewServer(serverChild2.Handler)
	defer httpServerChild2.Close()

	urlChild2, err := url.Parse(httpServerChild2.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	collFather1111.(*shared.StrongCollection).LeaderId = urlChild2.Host
	collChild1111.(*shared.StrongCollection).LeaderId = urlFather.Host

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

	var result string
	err = json.Unmarshal(bodyResp, &result)
	assert.NoError(t, err, "Expected no error when unmarshalling response body")
	assert.Equal(t, "function executed correctly", result, "Expected response body to be 'function executed correctly'")
}
