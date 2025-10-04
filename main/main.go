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
	// Create http logger for the HTTP Server component
	httpLog, err := logFactory.New("http_server")
	if err != nil {
		return fmt.Errorf("failed to create http logger: %v", err)
	}
	// Create metadata logger for MetadataManager component
	metadataLog, err := logFactory.New("metadata_manager")
	if err != nil {
		return fmt.Errorf("failed to create metadata logger: %v", err)
	}
	// Create redis logger for RedisClient component
	redisLog, err := logFactory.New("redis")
	if err != nil {
		return fmt.Errorf("failed to create redis logger: %v", err)
	}
	// Create cache logger for FunctionCache component
	cacheLog, err := logFactory.New("cache")
	if err != nil {
		return fmt.Errorf("failed to create cache logger: %v", err)
	}
	// Create data manager logger for DataManager component (includes MigrationManager and CollectionDiscoverer components)
	dataLog, err := logFactory.New("data")
	if err != nil {
		return fmt.Errorf("failed to create data logger: %v", err)
	}
	// Create scheduler logger for Scheduler component
	schedLog, err := logFactory.New("scheduler")
	if err != nil {
		return fmt.Errorf("failed to create scheduler logger: %v", err)
	}

	cfg := config.LoadConfig(configPath)
	lruCache := lru.LRUCacheConstructor[dataManagerShared.Collection](cfg.Lru.Size) //TODO: replace with redis size

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	serverInfo, err := info.NewServerInfo(cfg)
	if err != nil {
		return fmt.Errorf("error creating server info: %w", err)
	}

	httpServer, err := server.NewServer(httpLog, serverInfo)
	if err != nil {
		return err
	}

	redisClient := redis.NewRedisClient(cfg.Redis, ctx, redisLog)
	// remoteFunctionName must be in the format: "functionName/functionVersion"
	funcCache, err := caches.NewDefaultFunctionCache(cfg.FunctionsCache, cfg.Node.Secret, cacheLog)
	if err != nil {
		return fmt.Errorf("error creating function cache: %w", err)
	}
	tknDecoder, err := tokenDecoder.NewTokenDecoder("/path/to/pem/file")
	if err != nil {
		return fmt.Errorf("error creating token decoder: %w", err)
	}
	metadataLookup := metadataManager.NewMetadataManager(redisClient, serverInfo, metadataLog, funcCache)
	err = metadataLookup.RegisterRoutes(httpServer, tknDecoder)
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
	dataManager.Init(collections, penality.LogPenalty(2), cfg.MigrationManager, cfg.Lru, httpServer, *httpServer.ServerInfo, &lruCache, redisClient, dataLog)

	wasmExecutorPool := wasm.NewWasmedgeExecutorPool(10)
	err = scheduler.RegisterHostFns(wasmExecutorPool, scheduler.HostFnsDependencies{MetadataManager: metadataLookup, ServerInfo: serverInfo, DataManager: dataManager})
	if err != nil {
		return fmt.Errorf("error registering host functions: %w", err)
	}

	sched := scheduler.NewScheduler(dataManager, funcCache, wasmExecutorPool, serverInfo, schedLog)
	err = sched.RegisterRoutes(httpServer, tknDecoder, metadataLookup)
	if err != nil {
		return fmt.Errorf("error registering scheduler routes: %w", err)
	}

	go func() {
		generalLog.Infof("Server correctly created and listening on port %s", serverInfo.Port)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "error listening and serving: %s\n", err)
		}
	}()

	// TODO: understand these lines of code
	// I think it should be some synchronization method when the server shuts down
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
