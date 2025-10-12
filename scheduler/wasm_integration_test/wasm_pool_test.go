package wasm_integration_test

import (
	"fmt"
	metadataManager2 "metadataManager"
	datamanager_mock "mocks/dataManager"
	metadataManager_mock "mocks/metadataManager"
	"os"
	"scheduler"
	"strconv"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"

	"github.com/ermes-thesis/ermes-common/wasm-pool"

	"bou.ke/monkey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var authFn, fun, fibonacci, fibonacciWOpts, fibonacciAOT, primeNums, primeNumsWOpt, primeNumsAOT, lightweight, lightweightWOpts, lightweightAOT []byte

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
	fibonacciAOT, err = os.ReadFile("cpuIntensive/fibonacci/fibonacci_wOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}
	primeNums, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums.wasm")
	if err != nil {
		panic(err)
	}
	primeNumsWOpt, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums_wOpts.wasm")
	if err != nil {
		panic(err)
	}
	primeNumsAOT, err = os.ReadFile("cpuIntensive/primeNums/fnPrimeNums_wOpts_AOTwasmedge.wasm")
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
	lightweightAOT, err = os.ReadFile("lightweight/lightweight_wOpts_AOTwasmedge.wasm")
	if err != nil {
		panic(err)
	}

	m.Run()
}

func TestWasmPool_Wasmedge(t *testing.T) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	err := scheduler.RegisterWasmedgeHostFns(pool, scheduler.HostFnsDependencies{
		DataManager:     dataManager,
		MetadataManager: metadataManager,
		ServerInfo:      nil,
	})
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
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	err := scheduler.RegisterWasmtimeHostFns(pool, scheduler.HostFnsDependencies{
		DataManager:     dataManager,
		MetadataManager: metadataManager,
		ServerInfo:      nil,
	})
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
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	dataManager := datamanager_mock.NewDataManager(t)
	dataManager.EXPECT().GetRecord("255ef4af-3a49-4382-a90d-6524bdee5ad1", []string{"test"}, "battiti").Return("100", nil)
	dataManager.EXPECT().SetCollectionAndPropagate(mock.Anything, map[string]string{}, []string{"test", "pippo"}, false).Return(nil)

	metadataManager := metadataManager_mock.NewMetadataManager(t)
	metadataManager.EXPECT().CreateMetadata(mock.Anything, mock.AnythingOfType("Metadata"), metadataManager2.FROM_LOCAL).Return(nil)

	err := scheduler.RegisterWazeroHostFns(pool, scheduler.HostFnsDependencies{
		DataManager:     dataManager,
		MetadataManager: metadataManager,
		ServerInfo:      nil,
	})
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
//	pool := wasm.NewWasmedgeExecutorPool(10)
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
//	pool := wasm.NewWasmtimeExecutorPool(10)
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
//	pool := wasm.NewWasmedgeExecutorPool(10)
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
//	pool := wasm.NewWasmedgeExecutorPool(10)
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

func BenchmarkExecutionTime_Native_CpuIntensive(b *testing.B) {
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
		computePrimes(2000000)
	}
}

func BenchmarkExecutionTime_Native_Recursive(b *testing.B) {
	// Compute intensive fibonacci function
	var fibFun func(n int) int
	fibFun = func(n int) int {
		if n <= 1 {
			return n
		}
		return fibFun(n-1) + fibFun(n-2)
	}

	for b.Loop() {
		fibFun(40)
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
		simpleFun(20000)
	}
}

func BenchmarkExecutionTime_Wasmedge_NoOptimization_CpuIntensive(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_NoOptimization_Recursive(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_NoOptimization_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_WithOptimizations_CpuIntensive(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNumsWOpt, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_WithOptimizations_Recursive(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_WithOptimizations_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_CpuIntensive(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNumsAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_Recursive(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacciAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmedge_AOT_Lightweight(b *testing.B) {
	pool := wasm.NewWasmedgeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweightAOT, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_NoOptimization_CpuIntensive(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_NoOptimization_Recursive(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_NoOptimization_Lightweight(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_WithOptimizations_CpuIntensive(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNumsWOpt, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_WithOptimizations_Recursive(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wasmtime_WithOptimizations_Lightweight(b *testing.B) {
	pool := wasm.NewWasmtimeExecutorPool(10)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_NoOptimization_CpuIntensive(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNums, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_NoOptimization_Recursive(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacci, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_NoOptimization_Lightweight(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweight, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_WithOptimizations_CpuIntensive(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNumsWOpt, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_WithOptimizations_Recursive(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_WithOptimizations_Lightweight(b *testing.B) {
	pool := wasm.NewWazeroExecutorPool(10, false)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 0)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_WithOptimizations_CpuIntensive_Interpreter(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 2000000)
		response := executor.Invoke(primeNumsWOpt, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_WithOptimizations_Recursive_Interpreter(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(fibonacciWOpts, "entry", req, ctx)

		if response.HttpCode != 200 {
			b.Fatalf("invoke function failed: %s", response.Response)
		}
		executor.Release()
	}
}

func BenchmarkExecutionTime_Wazero_WithOptimizations_Lightweight_Interpreter(b *testing.B) {
	patch := monkey.Patch(wazero.NewRuntimeConfig, func() wazero.RuntimeConfig {
		return wazero.NewRuntimeConfigInterpreter()
	})
	defer patch.Unpatch()

	pool := wasm.NewWazeroExecutorPool(10, true)
	defer pool.Close()

	for b.Loop() {
		executor := pool.Acquire()

		req, ctx := getTestReqAndCtx("userId", 40)
		response := executor.Invoke(lightweightWOpts, "entry", req, ctx)

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
