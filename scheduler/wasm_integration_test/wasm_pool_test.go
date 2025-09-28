package wasm_integration_test

import (
	"fmt"
	metadataManager2 "metadataManager"
	datamanager_mock "mocks/dataManager"
	metadataManager_mock "mocks/metadataManager"
	"scheduler"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWasmPool(t *testing.T) {
	pool := wasm.NewExecutorPool(10)
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", "battiti", []string{"test"}).Return("100", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().AddGroupToCollection(mock.Anything, metadataManager2.FROM_LOCAL, []string{"testGroup"}).Return(nil)
	metadataManager.EXPECT().RemoveGroupFromCollection(mock.Anything, metadataManager2.FROM_LOCAL, []string{"testGroup"}).Return(nil)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	err := scheduler.RegisterHostFns(pool, scheduler.HostFnsDependencies{
		DataManager:     dataManager,
		MetadataManager: metadataManager,
		ServerInfo:      nil,
	})
	require.NoError(t, err)

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
	pool := wasm.NewExecutorPool(10)
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(b)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", "battiti", []string{"test"}).Return("100", nil)

	metadataManager := metadataManager_mock.NewMetadataManager(b)
	metadataManager.EXPECT().AddGroupToCollection(mock.Anything, metadataManager2.FROM_LOCAL, []string{"testGroup"}).Return(nil)
	metadataManager.EXPECT().RemoveGroupFromCollection(mock.Anything, metadataManager2.FROM_LOCAL, []string{"testGroup"}).Return(nil)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	scheduler.RegisterHostFns(pool, scheduler.HostFnsDependencies{
		MetadataManager: metadataManager,
		DataManager:     dataManager,
		ServerInfo:      nil,
	})

	for b.Loop() {
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
