package main

import (
	"config"
	"context"
	clDiscoverer "dataManager/collectionsDiscoverer"
	lru "dataManager/lru"
	migrationManager "dataManager/migrationManager"
	dataManagerShared "dataManager/shared"
	"db"
	"fmt"
	"io"
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

// TODO replace
func myPenalityFunc(bufferPos int, currentValue int) int {
	return currentValue
}

func run(ctx context.Context, w io.Writer, args []string) error {
	config := config.LoadConfig("../config/config.yaml")
	lruCache := lru.LRUCacheConstructor[dataManagerShared.Collection](config.Lru.Size)

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	serverInfo, err := info.NewServerInfo(args)
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

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "error listening and serving: %s\n", err)
		}
	}()

	//node set up
	collections := &dataManagerShared.CollectionPartialView{
		KnowedCollections: make(map[string]dataManagerShared.Collection),
	}
	migrationManagerIstance := migrationManager.GetMigrationManagerInstance()
	migrationManagerIstance.Init(collections, myPenalityFunc, config.MigrationManager, *ser.Serverinfo, &lruCache, redisClient)
	clDiscovererIstance := clDiscoverer.GetCollectionsDiscovererInstance()
	clDiscovererIstance.Init(collections)

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

	ctx := context.Background()

	if err := run(ctx, os.Stdout, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
