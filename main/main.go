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
	"server/info"
	"sync"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/rs/zerolog"

	"github.com/ermes-thesis/ermes-common/wasm-pool"
)

func run(ctx context.Context, configPath string) error {
	logFactory := logger.NewFactory(logger.Config{
		Level:  zerolog.InfoLevel,
		Format: "text",
		Output: &logger.LogFile{
			LogDir: "genLogs",
		},
		StdoutEnabled: true,
		AddSource:     true,
		Colored:       true,
	})

	// Create general logger
	generalLog, err := logFactory.New("general")
	if err != nil {
		return fmt.Errorf("failed to create general logger: %v", err)
	}
	defer generalLog.Close()

	generalLog.Info("Starting node...")

	cfg := config.LoadConfig(configPath)
	lruCache := lru.LRUCacheConstructor[dataManagerShared.Collection](cfg.Lru.Size) //TODO: replace with redis size

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	serverInfo, err := info.NewServerInfo(cfg)
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
	// remoteFunctionName must be in the format: "functionName/functionVersion"
	funcCache, err := caches.NewDefaultFunctionCache(cfg.FunctionsCache, cfg.Node.Secret, generalLog.Component("cache"))
	if err != nil {
		return fmt.Errorf("error creating function cache: %w", err)
	}
	tknDecoder, err := tokenDecoder.NewTokenDecoder(cfg.GroupTokenPublicKeyPath)
	if err != nil {
		return fmt.Errorf("error creating token decoder: %w", err)
	}
	metadataManager := metadataManager.NewMetadataManager(redisClient, serverInfo, generalLog.Component("metadata_manager"), funcCache)
	err = metadataManager.RegisterRoutes(httpServer, tknDecoder)
	if err != nil {
		return fmt.Errorf("error registering metadata routes: %w", err)
	}

	//node set up
	collections := &dataManagerShared.CollectionPartialView{
		KnownCollections: make(map[string]dataManagerShared.Collection),
	}
	err = collections.RestoreInfoFromSettings(cfg.Collections)
	if err != nil {
		return fmt.Errorf("error restoring collections from settings: %w", err)
	}
	dataManager := datamanager.GetDataManagerInstance()
	dataManager.Init(collections, penality.LogPenalty(2), cfg.MigrationManager, cfg.Lru, httpServer, *httpServer.ServerInfo, &lruCache, redisClient, generalLog.Component("data_manager"))

	var wasmExecutorPool *wasm.WasmExecutorPool
	hostFunctions := scheduler.NewHostFunctions(dataManager, metadataManager, serverInfo, generalLog.Component("host_functions"))
	switch cfg.Scheduler.WasmRuntime {
	case config.Wasmedge:
		wasmExecutorPool = wasm.NewWasmedgeExecutorPool(10)
		err = hostFunctions.RegisterWasmedgeHostFns(wasmExecutorPool)
	case config.Wasmtime:
		wasmExecutorPool = wasm.NewWasmtimeExecutorPool(10)
		err = hostFunctions.RegisterWasmtimeHostFns(wasmExecutorPool)
	case config.Wazero:
		wasmExecutorPool = wasm.NewWazeroExecutorPool(10, false)
		err = hostFunctions.RegisterWazeroHostFns(wasmExecutorPool)
	}
	if err != nil {
		return fmt.Errorf("error registering host functions: %w", err)
	}

	sched := scheduler.NewScheduler(dataManager, metadataManager, funcCache, wasmExecutorPool, serverInfo, generalLog.Component("scheduler"))
	err = sched.RegisterRoutes(httpServer, tknDecoder, metadataManager)
	if err != nil {
		return fmt.Errorf("error registering scheduler routes: %w", err)
	}

	go func() {
		generalLog.Infof("Server correctly created and listening on port %s", serverInfo.Port)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "error listening and serving: %s\n", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdownCtx := context.Background()
		shutdownCtx, newcancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer newcancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "error shutting down http server: %s\n", err)
		}
	}()

	wg.Wait()

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "No path specified for config file as argument, node cannot start.\n")
		os.Exit(0)
	}

	ctx := context.Background()

	if err := run(ctx, os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
