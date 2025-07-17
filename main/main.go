package main

import (
	"context"
	clDiscoverer "dataManager/collectionsDiscoverer"
	migrationManager "dataManager/migrationManager"
	dataManagerShared "dataManager/shared"
	"fmt"
	"io"
	"net"

	"net/http"
	"os"
	"os/signal"
	"server"
	"sync"
	"time"
)

// TODO replace
func myPenalityFunc(bufferPos int, currentValue int) int {
	return currentValue
}

func run(ctx context.Context, w io.Writer, args []string) error {
	loadConfig("../config/config.yaml")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	//node set up
	collections := &dataManagerShared.CollectionPartialView{
		KnowedCollections: make(map[string]dataManagerShared.Collection),
	}
	migrationManagerIstance := migrationManager.GetMigrationManagerInstance()
	migrationManagerIstance.Init(collections, myPenalityFunc)
	clDiscovererIstance := clDiscoverer.GetLookupManagerInstance()
	clDiscovererIstance.Init(collections)

	//Run server
	ser, err := server.NewServer(args, w)

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
