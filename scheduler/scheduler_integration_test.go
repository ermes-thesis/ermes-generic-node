package scheduler_test

import (
	"config"
	shared "dataManager/shared"
	"encoding/json"
	"io"
	caches_mock "mocks/caches"
	datamanager_mock "mocks/dataManager"
	collectionsDiscoverer_mock "mocks/dataManager/collectionsDiscoverer"
	migrationManager_mock "mocks/dataManager/migrationManager"
	wasm_mock "mocks/ermes-common/wasm-pool"
	metadataManager_mock "mocks/metadataManager"
	tokenDecoder_mock "mocks/tokenDecoder"
	"net/http"
	"net/http/httptest"
	"net/url"
	"scheduler"
	"server"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Constants & Global Configuration ---

const (
	fnName       = "test_function"
	fnVer        = "1.0.0"
	token        = "JWTToken"
	secret       = "secret"
	clientSecret = "client-secret"
	user         = "userId"
	queryName    = "private_data_query"
)

var defaultSchedulerConfig = config.SchedulerConfig{
	StartingOutdatedDataBackoff: time.Second,
	MaxOutdatedDataRetries:      10,
	MaxOutdatedDataBackoff:      time.Second * 5,
}

// --- WasmContext Builder ---

type ContextOption func(*scheduler.WasmContext)

// buildWasmContext creates a WasmContext with dynamic collections.
func buildWasmContext(opts ...ContextOption) scheduler.WasmContext {
	ctx := scheduler.WasmContext{
		UserId:    user,
		GroupList: []string{"groupC"},
		Collections: map[string][]model.CollectionInfo{
			queryName: {},
		},
	}
	for _, opt := range opts {
		opt(&ctx)
	}
	return ctx
}

// WithWeakCollection adds an eventual consistent collection to the context.
func WithWeakCollection(id string) ContextOption {
	return func(c *scheduler.WasmContext) {
		c.Collections[queryName] = append(c.Collections[queryName], model.CollectionInfo{
			Id: id,
			Metadata: model.Metadata{
				Name:        "collection_" + id,
				Owner:       user,
				Consistency: model.EventualConsistent,
				Geolocation: "IT/Como",
			},
		})
	}
}

// WithStrongCollection adds a sequential consistent collection to the context.
func WithStrongCollection(id string) ContextOption {
	return func(c *scheduler.WasmContext) {
		c.Collections[queryName] = append(c.Collections[queryName], model.CollectionInfo{
			Id: id,
			Metadata: model.Metadata{
				Name:        "collection_" + id,
				Owner:       user,
				Consistency: model.SequentialConsistent,
				Geolocation: "IT",
			},
		})
	}
}

// --- Test Infrastructure Helpers ---

func createNewLogger(t *testing.T) logger.Logger {
	lvl := logger.ErrorLevel
	if testing.Verbose() {
		lvl = logger.DebugLevel
	}
	log, err := logger.NewLogger(logger.Config{
		Level:     lvl,
		Format:    "text",
		AddSource: true,
	})
	require.NoError(t, err)
	return log
}

type TestNode struct {
	Server     *server.Server
	HttpServer *httptest.Server
	Host       string
	Scheduler  scheduler.Scheduler
	Logger     logger.Logger

	// Mocks
	FnCache      *caches_mock.FunctionCache
	DataManager  *datamanager_mock.DataManager
	MetaManager  *metadataManager_mock.MetadataManager
	TokenDecoder *tokenDecoder_mock.TokenDecoder
	WasmPool     *wasm_mock.ExecutorPool
	ColDisc      *collectionsDiscoverer_mock.CollectionsDiscoverer
	MigManager   *migrationManager_mock.MigrationManager
}

func (n *TestNode) Close() {
	n.HttpServer.Close()
}

// CollectionState defines how a collection exists on a node.
type CollectionState struct {
	IsPresent bool
	IsStrong  bool
	IsLeader  bool
	LeaderID  string // Hostname of the leader (if Strong)
}

// SetupCollection configures the mock behavior for a specific collection on this node.
// It returns the created internal shared.Collection object for further manipulation if needed.
func (n *TestNode) SetupCollection(id string, state *CollectionState) shared.Collection {
	if state == nil {
		n.ColDisc.EXPECT().IsCollectionLocallyPresent(id, uint64(0)).Return(false, nil, nil)
		return nil
	}

	var coll shared.Collection

	// 1. Create the appropriate Collection object
	if state.IsStrong {
		coll = shared.NewStrongCollection(id, 40, !state.IsPresent, state.IsLeader, state.LeaderID)
		coll.(*shared.StrongCollection).LeaderId = state.LeaderID
	} else {
		coll = shared.NewWeakCollection(id, 40, !state.IsPresent)
	}

	// 2. Configure Mock Expectations
	if state.IsPresent {
		// Simulate Read Lock which is often required by the scheduler when checking state
		if localColl, ok := coll.GetCollectionState().(*shared.LocalCollection); state.IsStrong && ok {
			localColl.CollectionLock.RLock()
		}
		n.ColDisc.EXPECT().IsCollectionLocallyPresent(id, uint64(0)).Return(true, coll, nil)
	} else {
		n.ColDisc.EXPECT().IsCollectionLocallyPresent(id, uint64(0)).Return(false, coll, nil)
	}

	return coll
}

func setupTestNode(t *testing.T, log logger.Logger) *TestNode {
	fnCache := caches_mock.NewFunctionCache(t)
	tokenDecoder := tokenDecoder_mock.NewTokenDecoder(t)
	dataManager := datamanager_mock.NewDataManager(t)
	colDisc := collectionsDiscoverer_mock.NewCollectionsDiscoverer(t)
	migManager := migrationManager_mock.NewMigrationManager(t)
	metaManager := metadataManager_mock.NewMetadataManager(t)
	wasmPool := wasm_mock.NewExecutorPool(t)

	serverInfo := &server.ServerInfo{
		MetadataDomain: "IT",
		Secret:         secret,
		ClientSecret:   clientSecret,
		Children:       []server.Node{},
	}

	sched := scheduler.NewScheduler(true, dataManager, metaManager, fnCache, wasmPool, serverInfo, defaultSchedulerConfig, log)
	srv, err := server.NewServer(log, serverInfo)
	require.NoError(t, err)

	err = sched.RegisterRoutes(srv, tokenDecoder)
	require.NoError(t, err)

	httpServer := httptest.NewServer(srv.Handler)
	parsedURL, _ := url.Parse(httpServer.URL)
	serverInfo.HostName = parsedURL.Host

	// Default expectations for getting components
	dataManager.EXPECT().GetCollectionsDiscoverer().Return(colDisc).Maybe()
	dataManager.EXPECT().GetMigrationManager().Return(migManager).Maybe()

	return &TestNode{
		Server:       srv,
		HttpServer:   httpServer,
		Host:         parsedURL.Host,
		Scheduler:    sched,
		Logger:       log,
		FnCache:      fnCache,
		DataManager:  dataManager,
		MetaManager:  metaManager,
		TokenDecoder: tokenDecoder,
		WasmPool:     wasmPool,
		ColDisc:      colDisc,
		MigManager:   migManager,
	}
}

func connectNodes(father, child *TestNode) {
	father.Server.ServerInfo.Children = append(father.Server.ServerInfo.Children, server.Node{HostName: child.Host})
	child.Server.ServerInfo.Father = &server.Node{HostName: father.Host}
}

func executeInvokeRequest(t *testing.T, targetNode *TestNode, ctx scheduler.WasmContext) string {
	reqUrl := targetNode.HttpServer.URL + "/invoke-function/" + fnName + "/" + fnVer
	req, err := http.NewRequest(http.MethodPost, reqUrl, nil)
	require.NoError(t, err)

	cachedQueries, _ := json.Marshal(ctx.Collections)
	req.Header.Set("X-CLIENT-SECRET", clientSecret)
	req.Header.Set("X-ERMES-CACHED-QUERY", string(cachedQueries))
	req.Header.Set("X-ERMES-GROUPS", token)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode)
	bodyResp, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	var result string
	json.Unmarshal(bodyResp, &result)
	return result
}

// --- Test Cases ---

// TestScheduleOnChild_ReadStrong: Child has both collections locally,
// request sent to Child and executed there.
func TestScheduleOnChild_ReadStrong(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// 1. Build Context: Needs 1 Weak + 1 Strong
	wasmCtx := buildWasmContext(
		WithWeakCollection("12345"),
		WithStrongCollection("1111"),
	)
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// 2. Setup Node Data (Collections)
	// Child has both collections locally
	child.SetupCollection("12345", &CollectionState{IsPresent: true, IsStrong: false})
	child.SetupCollection("1111", &CollectionState{IsPresent: true, IsStrong: true, IsLeader: false})

	// 3. Setup Mocks
	child.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{
		Name: fnName, Version: fnVer, StrongCollWriteAccess: false,
	}, nil)
	child.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	child.DataManager.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	child.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345", "1111"}).Return()
	child.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, "user").Return()

	child.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	// 4. Execute
	res := executeInvokeRequest(t, child, wasmCtx)
	assert.Equal(t, "ok", res)
}

// TestScheduleOnFather_WriteStrong: Child has both collections locally,
// request sent to Child and executed on the leader (Father).
func TestScheduleOnFather_WriteStrong(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// 1. Context
	wasmCtx := buildWasmContext(
		WithWeakCollection("12345"),
		WithStrongCollection("1111"),
	)
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// 2. Setup Father Data
	// Father Missing Weak "12345"
	collWeakFather := father.SetupCollection("12345", &CollectionState{IsPresent: false, IsStrong: false})
	collWeakFather.AddHost(child.Host)
	// Father is Leader of Strong "1111"
	collStrongFather := father.SetupCollection("1111", &CollectionState{IsPresent: true, IsStrong: true, IsLeader: true})
	collStrongFather.AddHost(child.Host)

	// 3. Father Mocks
	father.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{
		Name: fnName, Version: fnVer, StrongCollWriteAccess: true,
	}, nil)
	father.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	father.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	father.DataManager.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)
	father.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345", "1111"}).Return()
	father.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, child.Host).Return()

	father.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	// 4. Setup Child Data
	// Child has Weak "12345"
	child.SetupCollection("12345", &CollectionState{IsPresent: true, IsStrong: false})
	// Child has Strong "1111" (Follower, pointing to Father)
	collStrongChild := child.SetupCollection("1111", &CollectionState{IsPresent: true, IsStrong: true, IsLeader: false, LeaderID: father.Host})

	// 5. Child Mocks
	child.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{StrongCollWriteAccess: true}, nil)
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	child.MigManager.EXPECT().HandlePostponedDecisions(collStrongChild).Return()

	// 6. Execute (Request to Child, handled via Father logic due to WriteStrong on Leader)
	res := executeInvokeRequest(t, child, wasmCtx)
	assert.Equal(t, "ok", res)
}

// TestScheduleOnChild_WeakReplicated: Child has the Weak collection locally,
// request sent to Child and executed there.
func TestScheduleOnChild_WeakReplicated(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// 1. Context: Only Weak
	wasmCtx := buildWasmContext(WithWeakCollection("12345"))
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// 2. Child Data
	child.SetupCollection("12345", &CollectionState{IsPresent: true, IsStrong: false})

	// 3. Child Mocks
	child.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	child.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345"}).Return()
	child.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, "user").Return()

	child.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	res := executeInvokeRequest(t, child, wasmCtx)
	assert.Equal(t, "ok", res)
}

// TestScheduleOnChild_DoubleWeak: Child has one Weak collection locally, the other is missing.
func TestScheduleOnChild_DoubleWeak(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// 1. Context: Two Weak Collections
	wasmCtx := buildWasmContext(
		WithWeakCollection("12345"),
		WithWeakCollection("54321"),
	)
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// 2. Child Data
	child.SetupCollection("12345", &CollectionState{IsPresent: true, IsStrong: false})
	// 54321 is missing
	child.SetupCollection("54321", nil)

	// 3. Child Mocks
	child.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	child.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345", "54321"}).Return()
	child.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, "user").Return()

	child.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	res := executeInvokeRequest(t, child, wasmCtx)
	assert.Equal(t, "ok", res)
}

// TestScheduleOnFather_EmptyChildWeak: Father has the Weak collection locally,
// Child is missing it. Request sent to Child, but execution happens on Father.
func TestScheduleOnFather_EmptyChildWeak(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// 1. Context
	wasmCtx := buildWasmContext(WithWeakCollection("12345"))
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// 2. Father Data
	collFather := father.SetupCollection("12345", &CollectionState{IsPresent: true, IsStrong: false})
	collFather.AddHost(child.Host)

	// 3. Father Mocks
	father.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	father.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	father.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345"}).Return()
	father.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, child.Host).Return()

	father.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	// 4. Child Data (Missing)
	child.SetupCollection("12345", nil)

	// 5. Child Mocks
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	res := executeInvokeRequest(t, child, wasmCtx)
	assert.Equal(t, "ok", res)
}

// TestScheduleOnChild_EmptyFather: Father has no data, Child has it.
// Request sent to Father, but execution happens on Child.
func TestScheduleOnChild_EmptyFather(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// Context: 1 Weak
	wasmCtx := buildWasmContext(WithWeakCollection("12345"))
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// Father Data: Does NOT have "12345"
	collFather := father.SetupCollection("12345", &CollectionState{IsPresent: false})
	collFather.AddHost(child.Host)

	// Father Expectations (Receives request, checks local, fails, probes child)
	father.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	// Child Data: Has "12345"
	child.SetupCollection("12345", &CollectionState{IsPresent: true})

	// Child Expectations (Receives probe/request, executes)
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	child.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)

	child.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345"}).Return()
	child.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, father.Host).Return()

	child.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	// Act: Send request to FATHER
	res := executeInvokeRequest(t, father, wasmCtx)
	assert.Equal(t, "ok", res)
}

// TestScheduleOnChild_WriteStrong: Request to Father, but Child is Strong Leader.
// Execution must happen on Child.
func TestScheduleOnChild_WriteStrong(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child := setupTestNode(t, log.With("node", "child"))
	defer child.Close()
	connectNodes(father, child)

	// Context: 1 Weak + 1 Strong
	wasmCtx := buildWasmContext(WithWeakCollection("12345"), WithStrongCollection("1111"))
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// Father Data: Has Weak "12345", Has Strong "1111" (Follower)
	father.SetupCollection("12345", &CollectionState{IsPresent: true})
	father.SetupCollection("1111", &CollectionState{IsPresent: true, IsStrong: true, IsLeader: false, LeaderID: child.Host})

	// Father Expectations (Checks permissions, finds it's not leader)
	father.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{
		Name: fnName, Version: fnVer, StrongCollWriteAccess: true,
	}, nil)
	father.MigManager.EXPECT().HandlePostponedDecisions(mock.Anything).Return()
	father.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	// Child Data: Missing Weak "12345", Has Strong "1111" (Leader)
	child.SetupCollection("12345", nil)
	child.SetupCollection("1111", &CollectionState{IsPresent: true, IsStrong: true, IsLeader: true, LeaderID: child.Host})

	// Child Expectations (Executes)
	child.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{
		Name: fnName, Version: fnVer, StrongCollWriteAccess: true,
	}, nil)
	child.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	child.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	child.DataManager.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	child.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"12345", "1111"}).Return()
	child.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, father.Host).Return()

	child.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	// Act: Send request to FATHER
	res := executeInvokeRequest(t, father, wasmCtx)
	assert.Equal(t, "ok", res)
}

func TestScheduleThreeNodes(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father := setupTestNode(t, log.With("node", "father"))
	defer father.Close()
	child1 := setupTestNode(t, log.With("node", "child1"))
	defer child1.Close()
	child2 := setupTestNode(t, log.With("node", "child2"))
	defer child2.Close()
	connectNodes(father, child1)
	connectNodes(father, child2)

	// 1. Context: Strong Collection "1111"
	wasmCtx := buildWasmContext(WithStrongCollection("1111"))
	wasmCtxJson, _ := json.Marshal(wasmCtx)

	// 2. Node Data
	father.SetupCollection("1111", &CollectionState{IsPresent: false, IsStrong: true, LeaderID: child2.Host})
	child1.SetupCollection("1111", nil)
	child2.SetupCollection("1111", &CollectionState{IsPresent: true, IsStrong: true, IsLeader: true})

	// 3. Mocks
	// Father
	father.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{
		Name: fnName, Version: fnVer, StrongCollWriteAccess: true,
	}, nil)
	father.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	// Child 1
	child1.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)

	// Child 2 (Target)
	child2.FnCache.EXPECT().GetMetadata(fnName, fnVer).Return(&model.FunctionMetadata{
		Name: fnName, Version: fnVer, StrongCollWriteAccess: true,
	}, nil)
	child2.FnCache.EXPECT().GetBinary(fnName, fnVer).Return([]byte("bin"), nil)
	child2.TokenDecoder.EXPECT().DecodeGroupToken(token).Return([]string{"groupC"}, user, nil)
	child2.DataManager.EXPECT().GetCollectionVectorClockValue("1111").Return(uint64(0), nil)

	child2.MigManager.EXPECT().UpdateCollectionsLatestUsage([]string{"1111"}).Return()
	child2.MigManager.EXPECT().UpdatesRequestsInfo(mock.Anything, father.Host).Return()

	child2.WasmPool.EXPECT().Invoke([]byte("bin"), "entry", [][]byte{[]byte("{}"), wasmCtxJson}).Return(model.WasmResponse{
		HttpCode: 200, Response: "ok",
	})

	res := executeInvokeRequest(t, child1, wasmCtx)
	assert.Equal(t, "ok", res)
}
