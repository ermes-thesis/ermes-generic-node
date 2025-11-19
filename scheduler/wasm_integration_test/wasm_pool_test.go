package wasm_integration_test

import (
	"context"
	"db"
	"fmt"
	"metadataManager/propagation"
	datamanager_mock "mocks/dataManager"
	metadataManager_mock "mocks/metadataManager"
	"os"
	"scheduler"
	"server"
	"strconv"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func createNewLogger(t testing.TB) logger.Logger {
	log, err := logger.NewLogger(logger.Config{
		Level:     zerolog.DebugLevel,
		Format:    "text",
		AddSource: false,
		Muted:     true,
	})
	require.NoError(t, err)
	return log
}

func TestWasmPool_Wasmedge(t *testing.T) {
	log, logErr := logger.NewLogger(logger.Config{
		Level:         zerolog.InfoLevel,
		Format:        "text",
		Output:        nil,
		StdoutEnabled: false,
		AddSource:     false,
		Colored:       false,
		Rolling:       false,
		Muted:         true,
	})
	require.NoError(t, logErr)
	authFn, err := os.ReadFile("auth.wasm")
	require.NoError(t, err)
	fun, err := os.ReadFile("fun.wasm")
	require.NoError(t, err)
	pool := wasm.NewWasmedgeExecutorPool(wasm.Config{
		PoolSize:  10,
		Timeout:   5 * time.Second,
		WithCache: false,
		IsAot:     false,
	}, createNewLogger(t))
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", "battiti").Return("100", nil)
	dataManager.EXPECT().GetRecords("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", []string{"testMGET", "testMGET2"}).Return([]any{"mget_test", "mget_test2"}, nil)
	dataManager.EXPECT().GetRecords(mock.Anything, []string{"test"}, "userId", []string{"cicico"}).Return([]any{"what_cicico_means"}, nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "nuova_collection", map[string]db.RecordValue{}, []string{"test", "pippo"}, "userId", false, false).Return(nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "multiple_get_coll", map[string]db.RecordValue{}, []string{"test", "fake"}, "userId", false, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	notify, wait := utils.WaitForCalls(t, 2, 5*time.Second)
	defer wait()

	metadataManager.
		EXPECT().
		CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), propagation.FROM_LOCAL).
		Return(nil).
		Run(func(collectionId string, metadata model.Metadata, reqOrigin propagation.PropagationOrigin) {
			notify()
		})

	hostFns := scheduler.NewHostFunctions(dataManager, metadataManager, &server.ServerInfo{ExistingDomains: map[string]struct{}{"IT/Milan": {}, "IT": {}}}, log)

	err = hostFns.RegisterWasmedgeHostFns(pool)
	require.NoError(t, err)

	resp := pool.Invoke(authFn, "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string), 0)

	response := pool.Invoke(fun, "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

func TestWasmPool_Wasmtime(t *testing.T) {
	log, logErr := logger.NewLogger(logger.Config{
		Level:         zerolog.InfoLevel,
		Format:        "text",
		Output:        nil,
		StdoutEnabled: false,
		AddSource:     false,
		Colored:       false,
		Rolling:       false,
		Muted:         true,
	})
	require.NoError(t, logErr)
	authFn, err := os.ReadFile("auth.wasm")
	require.NoError(t, err)
	fun, err := os.ReadFile("fun.wasm")
	require.NoError(t, err)
	pool := wasm.NewWasmtimeExecutorPool(wasm.Config{
		PoolSize:  10,
		Timeout:   5 * time.Second,
		WithCache: false,
		IsAot:     false,
	}, createNewLogger(t))
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", "battiti").Return("100", nil)
	dataManager.EXPECT().GetRecords("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", []string{"testMGET", "testMGET2"}).Return([]any{"mget_test", "mget_test2"}, nil)
	dataManager.EXPECT().GetRecords(mock.Anything, []string{"test"}, "userId", []string{"cicico"}).Return([]any{"what_cicico_means"}, nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "nuova_collection", map[string]db.RecordValue{}, []string{"test", "pippo"}, "userId", false, false).Return(nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "multiple_get_coll", map[string]db.RecordValue{}, []string{"test", "fake"}, "userId", false, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	notify, wait := utils.WaitForCalls(t, 2, 5*time.Second)
	defer wait()

	metadataManager.
		EXPECT().
		CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), propagation.FROM_LOCAL).
		Return(nil).
		Run(func(collectionId string, metadata model.Metadata, reqOrigin propagation.PropagationOrigin) {
			notify()
		})

	hostFns := scheduler.NewHostFunctions(dataManager, metadataManager, &server.ServerInfo{ExistingDomains: map[string]struct{}{"IT/Milan": {}, "IT": {}}}, log)

	err = hostFns.RegisterWasmtimeHostFns(pool)
	require.NoError(t, err)

	resp := pool.Invoke(authFn, "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string), 0)

	response := pool.Invoke(fun, "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

func TestWasmPool_Wazero(t *testing.T) {
	log, logErr := logger.NewLogger(logger.Config{
		Level:         zerolog.InfoLevel,
		Format:        "text",
		Output:        nil,
		StdoutEnabled: false,
		AddSource:     false,
		Colored:       false,
		Rolling:       false,
		Muted:         true,
	})
	require.NoError(t, logErr)
	authFn, err := os.ReadFile("auth.wasm")
	require.NoError(t, err)
	fun, err := os.ReadFile("fun.wasm")
	require.NoError(t, err)
	pool := wasm.NewWazeroExecutorPool(wasm.Config{
		PoolSize:  10,
		Timeout:   5 * time.Second,
		WithCache: false,
		IsAot:     false,
	}, context.Background(), createNewLogger(t))
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", "battiti").Return("100", nil)
	dataManager.EXPECT().GetRecords("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", []string{"testMGET", "testMGET2"}).Return([]any{"mget_test", "mget_test2"}, nil)
	dataManager.EXPECT().GetRecords(mock.Anything, []string{"test"}, "userId", []string{"cicico"}).Return([]any{"what_cicico_means"}, nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "nuova_collection", map[string]db.RecordValue{}, []string{"test", "pippo"}, "userId", false, false).Return(nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "multiple_get_coll", map[string]db.RecordValue{}, []string{"test", "fake"}, "userId", false, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	notify, wait := utils.WaitForCalls(t, 2, 5*time.Second)
	defer wait()

	metadataManager.
		EXPECT().
		CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), propagation.FROM_LOCAL).
		Return(nil).
		Run(func(collectionId string, metadata model.Metadata, reqOrigin propagation.PropagationOrigin) {
			notify()
		})
	hostFns := scheduler.NewHostFunctions(dataManager, metadataManager, &server.ServerInfo{ExistingDomains: map[string]struct{}{"IT/Milan": {}, "IT": {}}}, log)
	err = hostFns.RegisterWazeroHostFns(pool)
	require.NoError(t, err)

	resp := pool.Invoke(authFn, "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string), 0)

	response := pool.Invoke(fun, "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

func getTestReqAndCtx(userId string, inputN int) ([]byte, []byte) {
	req := "{\"prova\": \"ciao\", \"n\":\"" + strconv.FormatInt(int64(inputN), 10) + "\"}"

	timestamp, _ := time.Now().MarshalJSON()
	ctx := "{" +
		"\"userId\": \"" + userId + "\"," +
		"\"groupList\": [\"test\"]," +
		"\"collections\": {" +
		"\"test_collection\": [" +
		"{" +
		"\"Id\": \"255ef4af-3a49-4382-a90d-6524bdee5ad1\"," +
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
