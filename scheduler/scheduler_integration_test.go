package scheduler_test

import (
	shared "dataManager/shared"
	"encoding/json"
	"io"
	"metadataManager/types"
	"mocks/caches"
	datamanager_mock "mocks/dataManager"
	"mocks/dataManager/collectionsDiscoverer"
	migrationManager_mock "mocks/dataManager/migrationManager"
	wasm_mock "mocks/ermes-common/wasm-pool"
	wasm_executor_mock "mocks/ermes-common/wasm-pool/executor"
	"mocks/metadataManager"
	"mocks/tokenDecoder"
	"net/http"
	"net/http/httptest"
	"net/url"
	"scheduler"
	"server"
	"strings"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createNewLogger(t *testing.T) *logger.Logger {
	log, err := logger.NewLogger(logger.Config{
		Level:     zerolog.DebugLevel,
		Format:    "text",
		AddSource: true,
	})
	require.NoError(t, err)
	return log
}

func TestScheduleOnChild_ReadStrong(t *testing.T) {
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
						Geolocation: "IT/Como",
					},
				},
				{
					Id: "1111",
					Metadata: model.Metadata{
						Name:        "collectionNameFather",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745749000, 0),
						Groups:      []string{"groupC"},
						Consistency: model.SequentialConsistent,
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

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)

	dataManagerFather := datamanager_mock.NewDataManager(t)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: false,
	}, nil)
	fnCacheChild.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)
	collChild12345.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collChild1111 := shared.NewStrongCollection("1111", 40, false, false)
	collChild1111.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collChild12345, nil)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(true, collChild1111, nil)

	migrationManagerChild := migrationManager_mock.NewMigrationManager(t)
	migrationManagerChild.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)
	dataManagerChild.EXPECT().GetMigrationManager().Return(migrationManagerChild)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_executor_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	schedulerLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []server.Node{
		{
			HostName: httpServerChild.URL,
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
						Geolocation: "IT/Como",
					},
				},
				{
					Id: "1111",
					Metadata: model.Metadata{
						Name:        "collectionNameFather",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745749000, 0),
						Groups:      []string{"groupC"},
						Consistency: model.SequentialConsistent,
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
	fnCacheFather.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)
	fnCacheFather.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collFather12345 := shared.NewWeakCollection("12345", 40, true)
	collFather1111 := shared.NewStrongCollection("1111", 40, false, true)
	collFather1111.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(false, collFather12345, nil)
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(true, collFather1111, nil)

	migrationManagerFather := migrationManager_mock.NewMigrationManager(t)
	migrationManagerFather.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	dataManagerFather := datamanager_mock.NewDataManager(t)
	dataManagerFather.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererFather)
	dataManagerFather.EXPECT().GetMigrationManager().Return(migrationManagerFather)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorFather := wasm_executor_mock.NewExecutor(t)
	wasmExecutorFather.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorFather.EXPECT().Release().Return()

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolFather.EXPECT().Acquire().Return(wasmExecutorFather)

	schedulerLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
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
	collChild12345.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collChild1111 := shared.NewStrongCollection("1111", 40, false, false)
	collChild1111.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing url")
	collChild1111.(*shared.StrongCollection).LeaderId = urlFather.Host

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collChild12345, nil)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(true, collChild1111, nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing url")
	collFather12345.AddHost(urlChild.Host)
	collFather1111.AddHost(urlChild.Host)
	serverInfoFather.Children = []server.Node{
		{
			HostName: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
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

	dataManagerFather := datamanager_mock.NewDataManager(t)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_executor_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)
	collChild12345.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collChild12345, nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []server.Node{
		{
			HostName: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
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
	fnCacheFather.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collFather12345 := shared.NewWeakCollection("12345", 40, false)
	collFather12345.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collFather12345, nil)

	dataManagerFather := datamanager_mock.NewDataManager(t)
	dataManagerFather.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererFather)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorFather := wasm_executor_mock.NewExecutor(t)
	wasmExecutorFather.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorFather.EXPECT().Release().Return()

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolFather.EXPECT().Acquire().Return(wasmExecutorFather)

	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: urlFather.Host,
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(false, nil, nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []server.Node{
		{
			HostName: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
						Geolocation: "IT/Como",
					},
				},
				{
					Id: "54321",
					Metadata: model.Metadata{
						Name:        "collectionName2",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
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

	dataManagerFather := datamanager_mock.NewDataManager(t)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_executor_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345 := shared.NewWeakCollection("12345", 40, false)
	collChild12345.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collChild54321 := shared.NewWeakCollection("54321", 40, true)

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collChild12345, nil)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("54321", uint64(0)).Return(false, collChild54321, nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoFather.Children = []server.Node{
		{
			HostName: strings.Split(httpServerChild.URL, ":")[2],
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
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
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(false, collChild12345, nil)

	dataManagerFather := datamanager_mock.NewDataManager(t)
	dataManagerFather.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererFather)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_executor_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild12345Child := shared.NewWeakCollection("12345", 40, false)
	collChild12345Child.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collChild12345Child, nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	collChild12345.AddHost(urlChild.Host)

	req, err := http.NewRequest(http.MethodPost, httpServerFather.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "12345",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.EventualConsistent,
						Geolocation: "IT/Como",
					},
				},
				{
					Id: "1111",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.SequentialConsistent,
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
	fnCacheFather.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collFather12345 := shared.NewWeakCollection("12345", 40, false)
	collFather12345.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
	collFather1111 := shared.NewStrongCollection("1111", 40, false, false)
	collFather1111.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()

	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(true, collFather12345, nil)
	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(true, collFather1111, nil)

	dataManagerFather := datamanager_mock.NewDataManager(t)
	dataManagerFather.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererFather)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}

	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)
	fnCacheChild.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	tknDecoderChild := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild := wasm_executor_mock.NewExecutor(t)
	wasmExecutorChild.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild.EXPECT().Release().Return()

	wasmExecutorPoolChild := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild.EXPECT().Acquire().Return(wasmExecutorChild)

	collectionDiscovererChild := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild1111 := shared.NewStrongCollection("1111", 40, false, true)
	collChild1111.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(true, collChild1111, nil)
	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("12345", uint64(0)).Return(false, nil, nil)

	migrationManagerChild := migrationManager_mock.NewMigrationManager(t)
	migrationManagerChild.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)
	dataManagerChild.EXPECT().GetMigrationManager().Return(migrationManagerChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing url")

	collFather1111.AddHost(urlChild.Host)
	collFather1111.(*shared.StrongCollection).LeaderId = urlChild.Host

	req, err := http.NewRequest(http.MethodPost, httpServerFather.URL+"/invoke-function/test_function/1.0.0", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	cachedQueries, _ := json.Marshal(wasmContext.Collections)
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	wasmContext := scheduler.WasmContext{
		UserId:    "userId",
		GroupList: []string{"groupC"},
		Collections: map[string][]types.CollectionInfo{
			"private_data_query": {
				{
					Id: "1111",
					Metadata: model.Metadata{
						Name:        "collectionName",
						Owner:       "12345",
						CreatedAt:   time.Unix(1745748000, 0),
						Groups:      []string{"groupA", "groupB", "groupC"},
						Consistency: model.SequentialConsistent,
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
	fnCacheFather.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)

	collectionDiscovererFather := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collFather1111 := shared.NewStrongCollection("1111", 40, true, false)

	collectionDiscovererFather.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(false, collFather1111, nil)

	dataManagerFather := datamanager_mock.NewDataManager(t)
	dataManagerFather.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererFather)

	tknDecoderFather := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)

	wasmExecutorPoolFather := wasm_mock.NewExecutorPool(t)

	schedulerFatherLog := createNewLogger(t)
	schedulerFather := scheduler.NewScheduler(dataManagerFather, metadataManager, fnCacheFather, wasmExecutorPoolFather, serverInfoFather, schedulerFatherLog)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerFather.RegisterRoutes(serverFather, tknDecoderFather, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}

	fnCacheChild := caches_mock.NewFunctionCache(t)
	fnCacheChild.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
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

	collectionDiscovererChild.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(false, collChild1111, nil)

	dataManagerChild := datamanager_mock.NewDataManager(t)
	dataManagerChild.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild)

	schedulerChildLog := createNewLogger(t)
	schedulerChild := scheduler.NewScheduler(dataManagerChild, metadataManager, fnCacheChild, wasmExecutorPoolChild, serverInfoChild, schedulerChildLog)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild.RegisterRoutes(serverChild, tknDecoderChild, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	serverInfoChild2 := &server.ServerInfo{
		Father: &server.Node{
			HostName: strings.Split(httpServerFather.URL, ":")[2],
		},
		Children:       []server.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
		ClientSecret:   "client-secret",
	}

	fnCacheChild2 := caches_mock.NewFunctionCache(t)
	fnCacheChild2.EXPECT().GetMetadata(invokedFnName, invokedFnVer).Return(&model.FunctionMetadata{
		Name:                  invokedFnName,
		Version:               invokedFnVer,
		TemplateNames:         nil,
		DeployedOn:            nil,
		StrongCollWriteAccess: true,
	}, nil)
	fnCacheChild2.EXPECT().GetBinary(invokedFnName, invokedFnVer).Return([]byte("function binary"), nil)

	tknDecoderChild2 := tokenDecoder_mock.NewTokenDecoder(t)
	tknDecoderChild2.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	wasmExecutorChild2 := wasm_executor_mock.NewExecutor(t)
	wasmExecutorChild2.EXPECT().Invoke([]byte("function binary"), "entry", [][]byte{[]byte("{}"), wasmContextJson}).Return(model.WasmResponse{
		HttpCode: 200,
		Response: "function executed correctly",
	})
	wasmExecutorChild2.EXPECT().Release().Return()

	wasmExecutorPoolChild2 := wasm_mock.NewExecutorPool(t)
	wasmExecutorPoolChild2.EXPECT().Acquire().Return(wasmExecutorChild2)

	collectionDiscovererChild2 := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	collChild1111_2 := shared.NewStrongCollection("1111", 40, false, true)
	collChild1111_2.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()

	collectionDiscovererChild2.EXPECT().IsCollectionLocallyPresent("1111", uint64(0)).Return(true, collChild1111_2, nil)

	migrationManagerChild2 := migrationManager_mock.NewMigrationManager(t)
	migrationManagerChild2.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	dataManagerChild2 := datamanager_mock.NewDataManager(t)
	dataManagerChild2.EXPECT().GetCollectionsDiscoverer().Return(collectionDiscovererChild2)
	dataManagerChild2.EXPECT().GetMigrationManager().Return(migrationManagerChild2)

	schedulerChild2Log := createNewLogger(t)
	schedulerChild2 := scheduler.NewScheduler(dataManagerChild2, metadataManager, fnCacheChild2, wasmExecutorPoolChild2, serverInfoChild2, schedulerChild2Log)

	child2Log := createNewLogger(t)
	serverChild2, err := server.NewServer(child2Log, serverInfoChild2)
	assert.NoError(t, err, "Expected no error when creating server")

	err = schedulerChild2.RegisterRoutes(serverChild2, tknDecoderChild2, metadataManager)
	assert.NoError(t, err, "Expected no error when registering routes")

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
	req.Header.Set("X-CLIENT-SECRET", "client-secret")
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
