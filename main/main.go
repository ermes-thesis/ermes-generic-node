package main

import (
	"config"
	"context"
	"dataManager/lru"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	dataManagerShared "dataManager/shared"
	"db"
	"errors"
	"fmt"
	"io"
	"logger"
	"metadataManager"
	"net"
	"net/http"
	"os"
	"os/signal"
	"server"
	"server/info"
	"sync"
	"time"
)

func run(ctx context.Context, w io.Writer, configPath string) error {
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.INFO, &loggerFile)

	config := config.LoadConfig(configPath)
	lruCache := lru.LRUCacheConstructor[dataManagerShared.Collection](config.Lru.Size) //TODO: replace with redis size

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	serverInfo, err := info.NewServerInfo(config)
	if err != nil {
		return fmt.Errorf("error creating server info: %w", err)
	}

	redisClient := db.NewRedisClient(config.Redis, ctx)
	metadataLookup := metadataManager.NewMetadataManager(redisClient, serverInfo, w)

	//Run server
	ser, err := server.NewServer(w, serverInfo, redisClient, metadataLookup)

	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:    net.JoinHostPort("localhost", ser.Port),
		Handler: ser.Handler,
	}

	go func() {
		fmt.Fprintf(w, "Server correctly created and listening on port %s \n", ser.Port)

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "error listening and serving: %s\n", err)
		}
	}()

	//node set up
	collections := &dataManagerShared.CollectionPartialView{
		KnownCollections: make(map[string]dataManagerShared.Collection),
	}
	err = collections.RestoreInfoFromSettings(config.Collections, *logger)
	if err != nil {
		return fmt.Errorf("error restoring collections from settings: %w", err)
	}

	dataManger := migrationManager.GetMigrationManagerInstance(*logger)
	dataManger.Init(collections, penality.LogPenalty(2), config.MigrationManager, config.Lru, ser, *ser.Serverinfo, &lruCache, redisClient, *logger)

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
