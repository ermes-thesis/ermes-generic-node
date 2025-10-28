package wasm_integration_test

import (
	"context"
	"db"
	"fmt"
	metadataManager2 "metadataManager/propagation"
	datamanager_mock "mocks/dataManager"
	metadataManager_mock "mocks/metadataManager"
	"os"
	"scheduler"
	"server"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/tetratelabs/wazero"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/wasm-pool"

	"bou.ke/monkey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const primeNumsInput = 2000000
const fibonacciInput = 40
const lightweightInput = 20000

var authFn, fun, fibonacci, fibonacciWOpts, fibonacciAOT, fibonacciWOptsAOT, primeNums, primeNumsWOpts, primeNumsAOT, primeNumsWOptsAOT, lightweight, lightweightWOpts, lightweightAOT, lightweightWOptsAOT, tailFibonacci, tailFibonacciWOpts, tailFibonacciAOT, tailFibonacciWOptsAOT []byte
var log *logger.Logger

func TestMain(m *testing.M) {
	var err error

	authFn, err = os.ReadFile("authFunc/auth.wasm")
	if err != nil {
		panic(err)
	}
	fun, err = os.ReadFile("genericFunc/fun.wasm")
	if err != nil {
		panic(err)
	}
	fibonacci, err = os.ReadFile("cpuIntensive/fibonacci/fibonacci.wasm")
	if err != nil {
		panic(err)
	}
	fibonacciWOpts, err = os.ReadFile("cpuIntensive/fibonacci/fibonacci_wOpts.wasm")
	if err != nil {
		panic(err)
	}
	fibonacciAOT, err = os.ReadFile("cpuIntensive/fibonacci/fibonacci_noOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	fibonacciWOptsAOT, err = os.ReadFile("cpuIntensive/fibonacci/fibonacci_wOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	primeNums, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums.wasm")
	if err != nil {
		panic(err)
	}
	primeNumsWOpts, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums_wOpts.wasm")
	if err != nil {
		panic(err)
	}
	primeNumsAOT, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums_noOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	primeNumsWOptsAOT, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums_wOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	lightweight, err = os.ReadFile("lightweight/lightweight.wasm")
	if err != nil {
		panic(err)
	}
	lightweightWOpts, err = os.ReadFile("lightweight/lightweight_wOpts.wasm")
	if err != nil {
		panic(err)
	}
	lightweightAOT, err = os.ReadFile("lightweight/lightweight_noOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	lightweightWOptsAOT, err = os.ReadFile("lightweight/lightweight_wOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	tailFibonacci, err = os.ReadFile("cpuIntensive/tailFibonacci/tailFibonacci.wasm")
	if err != nil {
		panic(err)
	}
	tailFibonacciWOpts, err = os.ReadFile("cpuIntensive/tailFibonacci/tailFibonacci_wOpts.wasm")
	if err != nil {
		panic(err)
	}
	tailFibonacciAOT, err = os.ReadFile("cpuIntensive/tailFibonacci/tailFibonacci_noOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	tailFibonacciWOptsAOT, err = os.ReadFile("cpuIntensive/tailFibonacci/tailFibonacci_wOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}

	log, err = logger.NewLogger(logger.Config{
		Level:         zerolog.InfoLevel,
		Format:        "text",
		Output:        nil,
		StdoutEnabled: false,
		AddSource:     false,
		Colored:       false,
		Rolling:       false,
		Muted:         true,
	})
	if err != nil {
		panic(err)
	}

	m.Run()
}

func createNewLogger(t testing.TB) *logger.Logger {
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
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(t))
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", "battiti").Return("100", nil)
	dataManager.EXPECT().GetRecords("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", []string{"testMGET", "testMGET2"}).Return([]any{"mget_test", "mget_test2"}, nil)
	dataManager.EXPECT().GetRecords(mock.Anything, []string{"test"}, "userId", []string{"cicico"}).Return([]any{"what_cicico_means"}, nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "nuova_collection", map[string]db.RecordValue{}, []string{"test", "pippo"}, "userId", false, false).Return(nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "multiple_get_coll", map[string]db.RecordValue{}, []string{"test", "fake"}, "userId", false, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	hostFns := scheduler.NewHostFunctions(dataManager, metadataManager, &server.ServerInfo{ExistingDomains: map[string]struct{}{"IT/Milan": {}, "IT": {}}}, log)

	err := hostFns.RegisterWasmedgeHostFns(pool)
	require.NoError(t, err)

	executor := pool.Acquire()
	defer executor.Release()

	resp := executor.Invoke(authFn, "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string), 0)

	response := executor.Invoke(fun, "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

func TestWasmPool_Wasmtime(t *testing.T) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(t))
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", "battiti").Return("100", nil)
	dataManager.EXPECT().GetRecords("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", []string{"testMGET", "testMGET2"}).Return([]any{"mget_test", "mget_test2"}, nil)
	dataManager.EXPECT().GetRecords(mock.Anything, []string{"test"}, "userId", []string{"cicico"}).Return([]any{"what_cicico_means"}, nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "nuova_collection", map[string]db.RecordValue{}, []string{"test", "pippo"}, "userId", false, false).Return(nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "multiple_get_coll", map[string]db.RecordValue{}, []string{"test", "fake"}, "userId", false, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	hostFns := scheduler.NewHostFunctions(dataManager, metadataManager, &server.ServerInfo{ExistingDomains: map[string]struct{}{"IT/Milan": {}, "IT": {}}}, log)

	err := hostFns.RegisterWasmtimeHostFns(pool)
	require.NoError(t, err)

	executor := pool.Acquire()
	defer executor.Release()

	resp := executor.Invoke(authFn, "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string), 0)

	response := executor.Invoke(fun, "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

func TestWasmPool_Wazero(t *testing.T) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(t))
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", "battiti").Return("100", nil)
	dataManager.EXPECT().GetRecords("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "userId", []string{"testMGET", "testMGET2"}).Return([]any{"mget_test", "mget_test2"}, nil)
	dataManager.EXPECT().GetRecords(mock.Anything, []string{"test"}, "userId", []string{"cicico"}).Return([]any{"what_cicico_means"}, nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "nuova_collection", map[string]db.RecordValue{}, []string{"test", "pippo"}, "userId", false, false).Return(nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, "multiple_get_coll", map[string]db.RecordValue{}, []string{"test", "fake"}, "userId", false, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	hostFns := scheduler.NewHostFunctions(dataManager, metadataManager, &server.ServerInfo{ExistingDomains: map[string]struct{}{"IT/Milan": {}, "IT": {}}}, log)
	err := hostFns.RegisterWazeroHostFns(pool)
	require.NoError(t, err)

	executor := pool.Acquire()
	defer executor.Release()

	resp := executor.Invoke(authFn, "authEntry", []byte("token"))

	fmt.Println("Auth function:", resp.HttpCode, resp.Response)

	req, ctx := getTestReqAndCtx(resp.Response.(string), 0)

	response := executor.Invoke(fun, "entry", req, ctx)
	fmt.Println("Invoke function:", response.Response, response.HttpCode)

	if response.HttpCode != 200 {
		t.Fatalf("invoke function failed: %s", response.Response)
	}
}

//func BenchmarkWasmPool_all(b *testing.B) {
//	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
//	defer pool.Close()
//
//	dataManager := datamanager_mock.NewDataManager(b)
//	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
//	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)
//
//	metadataManager := metadataManager_mock.NewMetadataManager(b)
//	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)
//
//	scheduler.RegisterWasmedgeHostFns(pool, scheduler.HostFnsDependencies{
//		MetadataManager: metadataManager,
//		DataManager:     dataManager,
//		ServerInfo:      nil,
//	})
//
//	for b.Loop() {
//		executor := pool.Acquire()
//
//		resp := executor.Invoke("authFunc/auth_all", "authEntry", []byte("token"))
//		fmt.Println("Auth function:", resp.HttpCode, resp.Response)
//
//		req, ctx := getTestReqAndCtx(resp.Response.(string), 0)
//		response := executor.Invoke("genericFunct/fun_all", "entry", req, ctx)
//		fmt.Println("Invoke function:", response.Response, response.HttpCode)
//
//		if response.HttpCode != 200 {
//			b.Fatalf("invoke function failed: %s", response.Response)
//		}
//		executor.Release()
//	}
//}
//
//func BenchmarkWasmPool_all_Wasmtime(b *testing.B) {
//	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(t))
//	defer pool.Close()
//
//	dataManager := datamanager_mock.NewDataManager(b)
//	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
//	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)
//
//	metadataManager := metadataManager_mock.NewMetadataManager(b)
//	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)
//
//	scheduler.RegisterWasmtimeHostFns(pool, scheduler.HostFnsDependencies{
//		MetadataManager: metadataManager,
//		DataManager:     dataManager,
//		ServerInfo:      nil,
//	})
//
//	for b.Loop() {
//		executor := pool.Acquire()
//
//		resp := executor.Invoke("authFunc/auth_all", "authEntry", []byte("token"))
//		fmt.Println("Auth function:", resp.HttpCode, resp.Response)
//
//		req, ctx := getTestReqAndCtx(resp.Response.(string), 0)
//		response := executor.Invoke("genericFunc/fun_all", "entry", req, ctx)
//		fmt.Println("Invoke function:", response.Response, response.HttpCode)
//
//		if response.HttpCode != 200 {
//			b.Fatalf("invoke function failed: %s", response.Response)
//		}
//		executor.Release()
//	}
//}
//
//func BenchmarkWasmPool_AOT(b *testing.B) {
//	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
//	defer pool.Close()
//
//	dataManager := datamanager_mock.NewDataManager(b)
//	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
//	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)
//
//	metadataManager := metadataManager_mock.NewMetadataManager(b)
//	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)
//
//	scheduler.RegisterWasmedgeHostFns(pool, scheduler.HostFnsDependencies{
//		MetadataManager: metadataManager,
//		DataManager:     dataManager,
//		ServerInfo:      nil,
//	})
//
//	for b.Loop() {
//		executor := pool.Acquire()
//
//		resp := executor.Invoke("authFunc/auth_all", "authEntry", []byte("token"))
//		fmt.Println("Auth function:", resp.HttpCode, resp.Response)
//
//		req, ctx := getTestReqAndCtx(resp.Response.(string), 0)
//		response := executor.Invoke("genericFunc/fun_all", "entry", req, ctx)
//		fmt.Println("Invoke function:", response.Response, response.HttpCode)
//
//		if response.HttpCode != 200 {
//			b.Fatalf("invoke function failed: %s", response.Response)
//		}
//		executor.Release()
//	}
//}
//
//func BenchmarkWasmPool_all_Wazero(b *testing.B) {
//	pool := wasm.NewWazeroExecutorPool(10, false)
//	defer pool.Close()
//
//	dataManager := datamanager_mock.NewDataManager(b)
//	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", "battiti", []string{"test"}).Return("100", nil)
//	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)
//
//	metadataManager := metadataManager_mock.NewMetadataManager(b)
//	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)
//
//	err := scheduler.RegisterWazeroHostFns(pool, scheduler.HostFnsDependencies{
//		MetadataManager: metadataManager,
//		DataManager:     dataManager,
//		ServerInfo:      nil,
//	})
//	require.NoError(b, err)
//
//	for b.Loop() {
//		executor := pool.Acquire()
//
//		resp := executor.Invoke("authFunc/auth_aot", "authEntry", []byte("token"))
//		fmt.Println("Auth function:", resp.HttpCode, resp.Response)
//
//		req, ctx := getTestReqAndCtx(resp.Response.(string), 0)
//		response := executor.Invoke("genericFunc/fun_aot", "entry", req, ctx)
//		fmt.Println("Invoke function:", response.Response, response.HttpCode)
//
//		if response.HttpCode != 200 {
//			b.Fatalf("invoke function failed: %s", response.Response)
//		}
//		executor.Release()
//	}
//}
//
//func BenchmarkWasmPool_AOT_O3(b *testing.B) {
//	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
//	defer pool.Close()
//
//	dataManager := datamanager_mock.NewDataManager(b)
//	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
//	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)
//
//	metadataManager := metadataManager_mock.NewMetadataManager(b)
//	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)
//
//	err := scheduler.RegisterWasmedgeHostFns(pool, scheduler.HostFnsDependencies{
//		MetadataManager: metadataManager,
//		DataManager:     dataManager,
//		ServerInfo:      nil,
//	})
//	require.NoError(b, err)
//
//	for b.Loop() {
//		executor := pool.Acquire()
//
//		resp := executor.Invoke("authFunc/auth_aot_o3", "authEntry", []byte("token"))
//		fmt.Println("Auth function:", resp.HttpCode, resp.Response)
//
//		req, ctx := getTestReqAndCtx(resp.Response.(string), 0)
//		response := executor.Invoke("genericFunc/fun_aot_o3", "entry", req, ctx)
//		fmt.Println("Invoke function:", response.Response, response.HttpCode)
//
//		if response.HttpCode != 200 {
//			b.Fatalf("invoke function failed: %s", response.Response)
//		}
//		executor.Release()
//	}
//}

func BenchmarkExecutionTime_Native_PrimeNumbers(b *testing.B) {
	// Compute intensive prime numbers computation function
	var computePrimes func(n int) int
	computePrimes = func(n int) int {
		count := 0
		for i := 2; i <= n; i++ {
			isPrime := true
			for j := 2; j*j <= i; j++ {
				if i%j == 0 {
					isPrime = false
					break
				}
			}
			if isPrime {
				count++
			}
		}
		return count
	}

	for b.Loop() {
		computePrimes(primeNumsInput)
	}
}

func BenchmarkExecutionTime_Native_Fibonacci(b *testing.B) {
	// Compute intensive fibonacci function
	var fibFun func(n int) int
	fibFun = func(n int) int {
		if n <= 1 {
			return n
		}
		return fibFun(n-1) + fibFun(n-2)
	}

	for b.Loop() {
		fibFun(fibonacciInput)
	}
}

func BenchmarkExecutionTime_Native_Lightweight(b *testing.B) {
	// Lightweight computation function
	var simpleFun func(n int) int
	simpleFun = func(n int) int {
		count := 0
		for i := 0; i <= n; i++ {
			count++
		}
		return count
	}

	for b.Loop() {
		simpleFun(lightweightInput)
	}
}

func BenchmarkExecutionTime_Native_TailFibonacci(b *testing.B) {
	// Compute intensive fibonacci function
	var fibTailFun func(n, a, b int) int
	fibTailFun = func(n, a, b int) int {
		if n == 0 {
			return a
		}
		return fibTailFun(n-1, b, a+b)
	}

	for b.Loop() {
		fibTailFun(fibonacciInput, 0, 1)
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_NoOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_NoOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_NoOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_NoOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_WithOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNumsWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_WithOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_WithOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	executor := pool.Acquire()
	req, ctx := getTestReqAndCtx("userId", lightweightInput)

	for b.Loop() {
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
	}
	executor.Release()
}

func BenchmarkExecutionTime_Wasmedge_Interpreter_WithOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_NoOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNumsAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_NoOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacciAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_NoOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweightAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_NoOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacciAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_WithOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNumsWOptsAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_WithOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacciWOptsAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_WithOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweightWOptsAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_WithOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacciWOptsAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_NoOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_NoOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_NoOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_NoOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_WithOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNumsWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_WithOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_WithOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_JIT_WithOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10, createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_NoOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_NoOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_NoOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_NoOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_WithOpts_PrimeNumbers(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNumsWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_WithOpts_Fibonacci(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_WithOpts_Lightweight(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_AOT_WithOpts_TailFibonacci(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_WithOpts_PrimeNumbers(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNumsWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_WithOpts_Fibonacci(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_WithOpts_Lightweight(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_WithOpts_TailFibonacci(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_NoOpts_PrimeNumbers(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", primeNumsInput)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_NoOpts_Fibonacci(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_NoOpts_Lightweight(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", lightweightInput)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_Interpreter_NoOpts_TailFibonacci(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true, context.Background(), createNewLogger(b))
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", fibonacciInput)
		response := executor.Invoke(tailFibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
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
