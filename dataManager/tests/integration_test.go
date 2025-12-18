package tests

import (
	"config"
	datamanager "dataManager"
	"dataManager/lru"
	"dataManager/messages"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
	"db/test_utils"
	"net"
	"net/url"
	"server"
	"sync"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

var defaultMigrationConfig = config.MigrationManagerConfig{
	IdleTimeoutMs:     1e18,
	LeaderThreshold:   0,
	FollowerThreshold: 1,
	ViewSize:          3,
	GravityConstant:   0.981,
	StrongPolicy:      "ERMESPOLICY",
	WeakPolicy:        "ALWAYSCOPYPOLICY",
}

var defaultLruConfig = config.LruConfig{
	Size:      100000,
	SafeLevel: 0.8,
}

// testNode encapsulate all components of a test node
type testNode struct {
	Server      *server.Server
	HttpServer  *fasthttp.Server
	DataManager datamanager.DataManager
	Db          db.GenericDB
	URL         *url.URL
}

// setupOptions defines options for setting up a test node
type setupOptions struct {
	Name           string
	FatherHost     string
	HandlerWrapper func(routing.Handler) routing.Handler
}

// setupTestNode creates and configures a test node with the specified options
func setupTestNode(t *testing.T, log logger.Logger, opts setupOptions) (*testNode, func()) {
	// 1. Redis
	redisClient, shutdownRedis := test_utils.StartRedisTestContainer(t, log.Component("redis/"+opts.Name))

	// 2. Server Info
	info := &server.ServerInfo{
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
	}

	if opts.FatherHost != "" {
		info.Father = &server.Node{HostName: opts.FatherHost}
	}

	// 3. Server
	srv, err := server.NewServer(log, info)
	require.NoError(t, err)

	// 4. HTTP Handler
	var handler *routing.Router = srv.Handler
	ln, err := net.Listen("tcp", "127.0.0.1:0") // :0 = random free port
	if err != nil {
		panic(err)
	}

	httpServer := &fasthttp.Server{
		Handler: handler.HandleRequest,
	}

	go func() {
		if err := httpServer.Serve(ln); err != nil {
			if err != fasthttp.ErrConnectionClosed {
				panic(err)
			}
		}
	}()

	baseURL := "http://" + ln.Addr().String()

	// 5. Update Hostname post-bind
	parsedURL, err := url.Parse(baseURL)
	require.NoError(t, err)
	srv.ServerInfo.HostName = parsedURL.Host

	// 6. DataManager Dependencies
	collections := &shared.CollectionPartialView{
		KnownCollections: map[string]shared.Collection{},
		Mutex:            sync.Mutex{},
	}
	lruCache := lru.LRUCacheConstructor[shared.Collection](defaultLruConfig.Size)

	// 7. DataManager
	dm := datamanager.NewDataManagerForTest(
		collections,
		penality.LogPenalty(2),
		defaultMigrationConfig,
		defaultLruConfig,
		srv,
		*srv.ServerInfo,
		&lruCache,
		redisClient,
		log.With("node", opts.Name),
	)

	node := &testNode{
		Server:      srv,
		HttpServer:  httpServer,
		DataManager: dm,
		Db:          redisClient,
		URL:         parsedURL,
	}

	cleanup := func() {
		httpServer.Shutdown()
		shutdownRedis()
	}

	return node, cleanup
}

func TestUpdateAndReplica_OppositeDirections(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	// --- SETUP FATHER ---
	father, cleanupFather := setupTestNode(t, log, setupOptions{Name: "father"})
	defer cleanupFather()

	err := father.DataManager.SetCollectionAndPropagate("col1", "test", map[string]db.RecordValue{}, []string{}, "userId", false, false)
	require.NoError(t, err)

	// --- SETUP CHILD ---
	child, cleanupChild := setupTestNode(t, log, setupOptions{
		Name:       "child",
		FatherHost: father.Server.ServerInfo.HostName,
	})
	defer cleanupChild()

	// --- ACT ---
	father.DataManager.GetMigrationManager().UpdatesRequestsInfo([]messages.CollectionRequestInfo{
		{CollectionId: "col1"},
	}, child.URL.Host)

	err = child.DataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{
		{
			CollectionId: "col1",
			RecordNames:  []string{"record1"},
			RecordValues: []string{"value1"},
		},
	}, []string{}, "userId")
	require.NoError(t, err, "failed to update records in child")

	// --- ASSERT ---
	assert.Eventually(t, func() bool {
		val, err := father.Db.GetRecord("col1", "record1")
		return err == nil && val == "value1"
	}, 5*time.Second, 100*time.Millisecond, "record not found in father")

	assert.Eventually(t, func() bool {
		val, err := child.Db.GetRecord("col1", "record1")
		return err == nil && val == "value1"
	}, 5*time.Second, 100*time.Millisecond, "record not found in child")
}

func TestUpdateAndReplica_OppositeDirections_SlowReplica(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	// --- SETUP FATHER ---
	father, cleanupFather := setupTestNode(t, log, setupOptions{Name: "father"})
	defer cleanupFather()

	err := father.DataManager.SetCollectionAndPropagate("col1", "test", map[string]db.RecordValue{}, []string{}, "userId", false, false)
	require.NoError(t, err)

	// --- SETUP CHILD (SLOW) ---
	slowMiddleware := func(next routing.Handler) routing.Handler {
		return func(ctx *routing.Context) error {
			if string(ctx.Path()[:]) == "/pvt-repo" {
				log.Warn("Delaying /pvt-repo response...")
				time.Sleep(500 * time.Millisecond)
			}
			next(ctx)
			return nil
		}
	}

	child, cleanupChild := setupTestNode(t, log, setupOptions{
		Name:           "child",
		FatherHost:     father.Server.ServerInfo.HostName,
		HandlerWrapper: slowMiddleware,
	})
	defer cleanupChild()

	// --- ACT ---
	father.DataManager.GetMigrationManager().UpdatesRequestsInfo([]messages.CollectionRequestInfo{
		{CollectionId: "col1"},
	}, child.URL.Host)

	err = child.DataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{
		{
			CollectionId: "col1",
			RecordNames:  []string{"record1"},
			RecordValues: []string{"value1"},
		},
	}, []string{}, "userId")
	require.NoError(t, err, "failed to update records in child")

	// --- ASSERT ---
	assert.Eventually(t, func() bool {
		val, err := father.Db.GetRecord("col1", "record1")
		return err == nil && val == "value1"
	}, 5*time.Second, 100*time.Millisecond, "record not found in father")

	assert.Eventually(t, func() bool {
		val, err := child.Db.GetRecord("col1", "record1")
		return err == nil && val == "value1"
	}, 5*time.Second, 100*time.Millisecond, "record not found in child")
}

func TestUpdateAndReplica_OppositeDirections_SlowPropagation(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	// --- SETUP FATHER ---
	slowMiddleware := func(next routing.Handler) routing.Handler {
		return func(ctx *routing.Context) error {
			if string(ctx.Path()[:]) == "/pvt-dataUpdatePropagation" {
				log.Warn("Delaying /pvt-dataUpdatePropagation response...")
				time.Sleep(500 * time.Millisecond)
			}
			next(ctx)
			return nil
		}
	}

	father, cleanupFather := setupTestNode(t, log, setupOptions{Name: "father", HandlerWrapper: slowMiddleware})
	defer cleanupFather()

	err := father.DataManager.SetCollectionAndPropagate("col1", "test", map[string]db.RecordValue{}, []string{}, "userId", false, false)
	require.NoError(t, err)

	// --- SETUP CHILD ---
	child, cleanupChild := setupTestNode(t, log, setupOptions{
		Name:       "child",
		FatherHost: father.Server.ServerInfo.HostName,
	})
	defer cleanupChild()

	// --- ACT ---
	father.DataManager.GetMigrationManager().UpdatesRequestsInfo([]messages.CollectionRequestInfo{
		{CollectionId: "col1"},
	}, child.URL.Host)

	err = child.DataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{
		{
			CollectionId: "col1",
			RecordNames:  []string{"record1"},
			RecordValues: []string{"value1"},
		},
	}, []string{}, "userId")
	require.NoError(t, err, "failed to update records in child")

	// --- ASSERT ---
	assert.Eventually(t, func() bool {
		val, err := father.Db.GetRecord("col1", "record1")
		return err == nil && val == "value1"
	}, 5*time.Second, 100*time.Millisecond, "record not found in father")

	assert.Eventually(t, func() bool {
		val, err := child.Db.GetRecord("col1", "record1")
		return err == nil && val == "value1"
	}, 5*time.Second, 100*time.Millisecond, "record not found in child")
}
