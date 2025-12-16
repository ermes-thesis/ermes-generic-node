//go:build test
// +build test

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
	"net/http"
	"net/http/httptest"
	"net/url"
	"server"
	"sync"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	HttpServer  *httptest.Server
	DataManager datamanager.DataManager
	Db          db.GenericDB
	URL         *url.URL
}

// setupOptions defines options for setting up a test node
type setupOptions struct {
	Name           string
	FatherURL      string
	HandlerWrapper func(http.Handler) http.Handler
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

	if opts.FatherURL != "" {
		fatherUrlParsed, _ := url.Parse(opts.FatherURL)
		info.Father = &server.Node{HostName: fatherUrlParsed.Host}
	}

	// 3. Server
	srv, err := server.NewServer(log, info)
	require.NoError(t, err)

	// 4. HTTP Handler
	var handler http.Handler = srv.Handler
	if opts.HandlerWrapper != nil {
		handler = opts.HandlerWrapper(srv.Handler)
	}
	httpServer := httptest.NewServer(handler)

	// 5. Update Hostname post-bind
	parsedURL, err := url.Parse(httpServer.URL)
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
		httpServer.Close()
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
		Name:      "child",
		FatherURL: father.HttpServer.URL,
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
	slowMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/pvt-repo" {
				log.Warn("Delaying /pvt-repo response...")
				time.Sleep(500 * time.Millisecond)
			}
			next.ServeHTTP(w, r)
		})
	}

	child, cleanupChild := setupTestNode(t, log, setupOptions{
		Name:           "child",
		FatherURL:      father.HttpServer.URL,
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
	slowMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/pvt-dataUpdatePropagation" {
				log.Warn("Delaying /pvt-dataUpdatePropagation response...")
				time.Sleep(500 * time.Millisecond)
			}
			next.ServeHTTP(w, r)
		})
	}

	father, cleanupFather := setupTestNode(t, log, setupOptions{Name: "father", HandlerWrapper: slowMiddleware})
	defer cleanupFather()

	err := father.DataManager.SetCollectionAndPropagate("col1", "test", map[string]db.RecordValue{}, []string{}, "userId", false, false)
	require.NoError(t, err)

	// --- SETUP CHILD ---
	child, cleanupChild := setupTestNode(t, log, setupOptions{
		Name:      "child",
		FatherURL: father.HttpServer.URL,
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
