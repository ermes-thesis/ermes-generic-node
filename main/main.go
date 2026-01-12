package main

import (
	"caches"
	"config"
	"context"
	datamanager "dataManager"
	"dataManager/lru"
	penality "dataManager/migrationManager/penalityFunction"
	dataManagerShared "dataManager/shared"
	"db/redis"
	"errors"
	"fmt"
	"metadataManager"
	"net/http"
	"os"
	"os/signal"
	"scheduler"
	"server"
	"sync"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
)

func run(ctx context.Context, configPath string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	cfg := config.LoadConfig(configPath)

	logFactory := logger.NewFactory(cfg.Logger)

	// Create general logger
	generalLog, err := logFactory.New("general")
	if err != nil {
		return fmt.Errorf("failed to create general logger: %v", err)
	}
	defer generalLog.Close()

	generalLog.Info("Starting node...")

	lruCache := lru.LRUCacheConstructor[dataManagerShared.Collection](cfg.Lru.Size) //TODO: replace with redis size

	serverInfo, err := server.NewServerInfo(cfg)
	if err != nil {
		return fmt.Errorf("error creating server info: %w", err)
	}

	httpServer, err := server.NewServer(generalLog.Component("http_server"), serverInfo)
	if err != nil {
		return err
	}

	redisClient, err := redis.NewRedisClient(cfg.Redis, ctx, generalLog.Component("redis"))
	if err != nil {
		return fmt.Errorf("error creating redis client: %w", err)
	}

	//node set up
	collections := dataManagerShared.NewCollectionPartialView()
	err = collections.RestoreInfoFromSettings(cfg.Collections, *serverInfo)
	if err != nil {
		return fmt.Errorf("error restoring collections from settings: %w", err)
	}
	dataManager := datamanager.GetDataManagerInstance()
	dataManager.Init(collections, penality.LogPenalty(2), cfg.MigrationManager, cfg.Lru, httpServer, *httpServer.ServerInfo, &lruCache, redisClient, generalLog.Component("data_manager"))

	if !serverInfo.IsCentralStorage {
		funcCache, err := caches.NewDefaultFunctionCache(cfg.FunctionsCache, cfg.Node.Secret, generalLog.Component("cache"))
		if err != nil {
			return fmt.Errorf("error creating function cache: %w", err)
		}
		tknDecoder, err := tokenDecoder.NewTokenDecoder(cfg.GroupTokenPublicKeyPath)
		if err != nil {
			return fmt.Errorf("error creating token decoder: %w", err)
		}
		metadataManager, err := metadataManager.NewMetadataManager(redisClient, serverInfo, generalLog.Component("metadata_manager"), funcCache)
		if err != nil {
			return fmt.Errorf("error creating metadata manager: %w", err)
		}
		err = metadataManager.RegisterRoutes(httpServer, tknDecoder)
		if err != nil {
			return fmt.Errorf("error registering metadata routes: %w", err)
		}

		var wasmExecutorPool wasm.ExecutorPool
		hostFunctions := scheduler.NewHostFunctions(dataManager, metadataManager, serverInfo, generalLog.Component("host_functions"))
		switch cfg.Scheduler.WasmRuntime {
		case wasm.Wasmedge:
			wasmedgeExecutorPool := wasm.NewWasmedgeExecutorPool(cfg.Scheduler.WasmPoolConfig, generalLog.Component("wasmedge_executor_pool"))
			err = hostFunctions.RegisterWasmedgeHostFns(wasmedgeExecutorPool)
			wasmExecutorPool = wasmedgeExecutorPool
		case wasm.Wasmtime:
			wasmtimeExecutorPool := wasm.NewWasmtimeExecutorPool(cfg.Scheduler.WasmPoolConfig, generalLog.Component("wasmtime_executor_pool"))
			err = hostFunctions.RegisterWasmtimeHostFns(wasmtimeExecutorPool)
			wasmExecutorPool = wasmtimeExecutorPool
		case wasm.Wazero:
			wazeroExecutorPool := wasm.NewWazeroExecutorPool(cfg.Scheduler.WasmPoolConfig, ctx, generalLog.Component("wazero_executor_pool"))
			err = hostFunctions.RegisterWazeroHostFns(wazeroExecutorPool)
			wasmExecutorPool = wazeroExecutorPool
		}
		if err != nil {
			return fmt.Errorf("error registering host functions: %w", err)
		}

		sched := scheduler.NewScheduler(serverInfo.Stateful, dataManager, metadataManager, funcCache, wasmExecutorPool, serverInfo, cfg.Scheduler, generalLog.Component("scheduler"))
		err = sched.RegisterRoutes(httpServer, tknDecoder)
		if err != nil {
			return fmt.Errorf("error registering scheduler routes: %w", err)
		}
	}

	go func() {
		generalLog.Infof("Server correctly created and listening on port %s", serverInfo.Port)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			generalLog.Errorf("error starting http server: %v", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Go(func() {
		<-ctx.Done()
		shutdownCtx, newCancel := context.WithTimeout(context.Background(), 10*time.Second)
		dataManager.GetMigrationManager().StopAsyncDump()
		defer newCancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			generalLog.Errorf("error shutting down http server: %v", err)
		}
	})

	wg.Wait()

	generalLog.Infof("Exiting node gracefully")
	return nil
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintf(os.Stderr, "No path specified for config file as argument, node cannot start.\n")
		return
	}

	ctx := context.Background()

	if err := run(ctx, os.Args[1]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Fatal error running node: %v\n", err)
		os.Exit(1)
	}
}
