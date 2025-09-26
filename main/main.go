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
	"io"
	"logger"
	"metadataManager"
	"net"
	"net/http"
	"os"
	"os/signal"
	"scheduler"
	"server"
	"server/info"
	"sync"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
)

func run(ctx context.Context, w io.Writer, configPath string) error {
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var log = logger.NewLogger(logger.INFO, &loggerFile)

	cfg := config.LoadConfig(configPath)
	lruCache := lru.LRUCacheConstructor[dataManagerShared.Collection](cfg.Lru.Size) //TODO: replace with redis size

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	serverInfo, err := info.NewServerInfo(cfg)
	if err != nil {
		return fmt.Errorf("error creating server info: %w", err)
	}

	ser, err := server.NewServer(w, serverInfo)
	if err != nil {
		return err
	}

	redisClient := redis.NewRedisClient(cfg.Redis, ctx)
	// remoteFunctionName must be in the format: "functionName/functionVersion"
	funcCache, err := caches.NewDefaultFunctionCache(cfg.FunctionsCache, cfg.Node.Secret)
	if err != nil {
		return fmt.Errorf("error creating function cache: %w", err)
	}
	tknDecoder, err := tokenDecoder.NewTokenDecoder("/path/to/pem/file")
	if err != nil {
		return fmt.Errorf("error creating token decoder: %w", err)
	}
	metadataLookup := metadataManager.NewMetadataManager(redisClient, serverInfo, w, funcCache)
	err = metadataLookup.RegisterRoutes(ser, tknDecoder)
	if err != nil {
		return fmt.Errorf("error registering metadata routes: %w", err)
	}

	//node set up
	collections := &dataManagerShared.CollectionPartialView{
		KnownCollections: make(map[string]dataManagerShared.Collection),
	}
	err = collections.RestoreInfoFromSettings(cfg.Collections, *log)
	if err != nil {
		return fmt.Errorf("error restoring collections from settings: %w", err)
	}
	dataManager := datamanager.GetDataManagerInstance(*log)
	dataManager.Init(collections, penality.LogPenalty(2), cfg.MigrationManager, cfg.Lru, ser, *ser.ServerInfo, &lruCache, redisClient, *log)

	wasmExecutorPool := wasm.NewExecutorPool(10)
	err = scheduler.RegisterHostFns(wasmExecutorPool, scheduler.HostFnsDependencies{RedisClient: redisClient, MetadataManager: metadataLookup, ServerInfo: serverInfo})
	if err != nil {
		return fmt.Errorf("error registering host functions: %w", err)
	}

	sched := scheduler.NewScheduler(dataManager.GetCollectionsDiscoverer(), funcCache, wasmExecutorPool, serverInfo)
	err = sched.RegisterRoutes(ser, tknDecoder, metadataLookup)
	if err != nil {
		return fmt.Errorf("error registering scheduler routes: %w", err)
	}

	httpServer := &http.Server{
		Addr:    net.JoinHostPort("localhost", serverInfo.Port),
		Handler: ser.Handler,
	}

	go func() {
		fmt.Fprintf(w, "Server correctly created and listening on port %s \n", serverInfo.Port)

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

	if err := run(ctx, os.Stdout, os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
