package metadataManager_test

import (
	"bytes"
	"db"
	"encoding/json"
	"io"
	"metadataManager"
	"metadataManager/propagation"
	caches_mock "mocks/caches"
	tokenDecoder_mock "mocks/tokenDecoder"
	"net"
	"net/http"
	"net/url"
	"server"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// TestMetadataNode encapsulates all components of a test metadata node
type TestMetadataNode struct {
	Server          *server.Server
	HttpServer      *fasthttp.Server
	Host            string
	MetadataManager metadataManager.MetadataManager
	Db              db.GenericDB

	FnCache      *caches_mock.FunctionCache
	TokenDecoder *tokenDecoder_mock.TokenDecoder
}

// Close shuts down the test metadata node
func (n *TestMetadataNode) Close() {
	n.HttpServer.Shutdown()
}

// setupMetadataNode sets up a test metadata node with optional parent node
func setupMetadataNode(t *testing.T, log logger.Logger, domain string, parent *TestMetadataNode) (*TestMetadataNode, func()) {
	// 1. Setup Redis
	redisClient, shutdownRedis := setupRedis(t, log.Component("redis"), parent != nil)

	// 2. Create Mocks
	fnCache := caches_mock.NewFunctionCache(t)
	tknDecoder := tokenDecoder_mock.NewTokenDecoder(t)

	// 3. Setup Server Info
	serverInfo := &server.ServerInfo{
		MetadataDomain: domain,
		Secret:         secret,
		Children:       []server.Node{},
	}

	// 4. Setup Metadata Manager & Server
	mm, err := metadataManager.NewMetadataManager(redisClient, serverInfo, log.Component("metadata_manager"), fnCache)
	require.NoError(t, err)

	srv, err := server.NewServer(log.Component("server"), serverInfo)
	require.NoError(t, err)

	err = mm.RegisterRoutes(srv, tknDecoder)
	require.NoError(t, err)

	// 5. Start HTTP Server
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
	serverInfo.HostName = parsedURL.Host

	// 6. Link with Parent if provided
	if parent != nil {
		serverInfo.Father = &server.Node{HostName: parent.Host, MetadataDomain: parent.Server.ServerInfo.MetadataDomain}
		parent.Server.ServerInfo.Children = append(parent.Server.ServerInfo.Children, server.Node{
			HostName:       parsedURL.Host,
			MetadataDomain: domain,
		})
	}

	node := &TestMetadataNode{
		Server:          srv,
		HttpServer:      httpServer,
		Host:            parsedURL.Host,
		MetadataManager: mm,
		Db:              redisClient,
		FnCache:         fnCache,
		TokenDecoder:    tknDecoder,
	}

	cleanup := func() {
		httpServer.Shutdown()
		shutdownRedis()
	}

	return node, cleanup
}

func getTestMetadata(name string, geolocation string) model.Metadata {
	return model.Metadata{
		Name:           name,
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{groupC},
		Consistency:    model.EventualConsistent,
		Geolocation:    geolocation,
		CustomMetadata: map[string]string{},
	}
}

// --- Test Cases ---

// TestMetadataLookupIntegration tests metadata lookup propagation from child to father
func TestMetadataLookupIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	defer cleanupFather()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan", father)
	defer cleanupChild()

	q := templates["private_data_query"]
	father.FnCache.EXPECT().GetTemplate(testFnName, testFnVer, "private_data_query").Return(&q, nil)
	father.TokenDecoder.EXPECT().DecodeGroupToken(tokenString).Return([]string{groupC}, userID, nil)

	child.TokenDecoder.EXPECT().DecodeGroupToken(tokenString).Return([]string{groupC}, userID, nil)

	req, err := http.NewRequest(http.MethodPost, "http://"+child.Server.ServerInfo.HostName+"/pvt-lookup", nil)
	require.NoError(t, err, "Expected no error creating HTTP request")

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
	})
	req.Header.Set("X-ERMES-SECRET", secret)
	req.Header.Set("X-ERMES-QUERY", string(queryParams))
	req.Header.Set("X-ERMES-GROUPS", tokenString)
	req.Header.Set("X-ERMES-REQ-FUNCTION", testFnName)
	req.Header.Set("X-ERMES-REQ-FUNCTION-VERSION", testFnVer)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "Expected no error on HTTP request")
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected HTTP 200 OK response")
	bodyResp, err := io.ReadAll(res.Body)
	require.NoError(t, err, "Expected no error reading response body")

	var response map[uint64]model.QueryResult
	err = json.Unmarshal(bodyResp, &response)
	require.NoError(t, err, "Expected no error unmarshaling response body")
	assert.Len(t, response, 1, "Expected one query result in response")

	hash := model.NewQueryIdentifier("private_data_query", map[string]string{
		"name": "collectionNameFather",
	}).Hash()

	assert.Contains(t, response, hash, "Expected response to contain result for the private_data_query")
	collection := response[hash].Collections[0]
	assert.Equal(t, "1111", collection.Id, "Expected collection ID to match")
	assert.Equal(t, "collectionNameFather", collection.Metadata.Name, "Expected collection name to match")
	assert.Contains(t, collection.Metadata.Groups, groupC, "Expected groups to be 'groupC'")
}

// TestMetadataCreationFromChildIntegration tests metadata creation propagation from child to father
func TestMetadataCreationFromChildIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	defer cleanupFather()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan", father)
	defer cleanupChild()

	metadata := getTestMetadata("test", "IT/Milan")

	jsonMetadata, err := json.Marshal(metadata)
	require.NoError(t, err, "Expected no error marshaling metadata to JSON")

	req, err := http.NewRequest(http.MethodPost, "http://"+child.Server.ServerInfo.HostName+"/pvt-create-metadata/"+collectionId1, bytes.NewReader(jsonMetadata))
	require.NoError(t, err, "Expected no error creating HTTP request")
	req.Header.Set("X-ERMES-SECRET", secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "IT/Milan/test")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "Expected no error on HTTP request")
	res.Body.Close()

	assert.Equal(t, http.StatusCreated, res.StatusCode, "Expected HTTP 201 Created response")

	// Verify that the metadata was created in both father and child
	metaChild, err := child.Db.GetMetadata(collectionId1)
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata, *metaChild, "Metadata in child does not match expected")

	metaFather, err := father.Db.GetMetadata(collectionId1)
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Metadata in father does not match expected")
}

// TestMetadataCreationFromFatherIntegration tests metadata creation propagation from father to child
func TestMetadataCreationFromFatherIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	father.Server.ServerInfo.Father = &server.Node{
		//root node, so that the father can think that the message arrive from top
	}
	defer cleanupFather()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan", father)
	defer cleanupChild()

	metadata := getTestMetadata("test", "IT/Milan")

	jsonMetadata, err := json.Marshal(metadata)
	require.NoError(t, err, "Expected no error marshaling metadata to JSON")

	req, err := http.NewRequest(http.MethodPost, "http://"+father.Server.ServerInfo.HostName+"/pvt-create-metadata/"+collectionId1, bytes.NewReader(jsonMetadata))
	require.NoError(t, err, "Expected no error creating HTTP request")
	req.Header.Set("X-ERMES-SECRET", secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "Expected no error on HTTP request")
	res.Body.Close()

	assert.Equal(t, http.StatusCreated, res.StatusCode)

	// Verify that the metadata was created in both father and child
	metaChild, err := child.Db.GetMetadata(collectionId1)
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata, *metaChild, "Metadata in child does not match expected")

	metaFather, err := father.Db.GetMetadata(collectionId1)
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Metadata in father does not match expected")
}

// TestMetadataCreationFromLocalIntegration tests metadata creation propagation from local node to both father and child
func TestMetadataCreationFromLocalIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	// Topology: Father -> Local -> Child
	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	defer cleanupFather()

	local, cleanupLocal := setupMetadataNode(t, log.With("node", "child"), "IT/Milan", father)
	defer cleanupLocal()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan/Polimi", local)
	defer cleanupChild()

	// Test 1: Create metadata with geolocation of local node
	metadata := getTestMetadata("test", "IT/Milan")
	err := local.MetadataManager.CreateMetadata(collectionId1, metadata, propagation.FROM_LOCAL)
	require.NoError(t, err, "Expected no error creating metadata from local node")

	// Verify that the metadata was created only in father and local, not in child
	metaChild, err := child.Db.GetMetadata(collectionId1)
	assert.Error(t, err, "Metadata should not exist in child node")
	assert.Nil(t, metaChild, "Metadata in child should be nil")

	metaFather, err := father.Db.GetMetadata(collectionId1)
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Metadata in father does not match expected")

	metaLocal, err := local.Db.GetMetadata(collectionId1)
	assert.NoError(t, err, "Expected no error when getting metadata from local")
	assert.Equal(t, metadata, *metaLocal, "Metadata in local does not match expected")

	// Test 2: Create metadata with geolocation of child node
	metadata2 := getTestMetadata("test2", "IT/Milan/Polimi")
	err = local.MetadataManager.CreateMetadata("test2", metadata2, propagation.FROM_LOCAL)
	assert.NoError(t, err, "Expected no error creating metadata from local node")

	// Verify that the metadata was created in both father and child
	metaChild2, err := child.Db.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata2, *metaChild2, "Metadata in child does not match expected")

	metaFather2, err := father.Db.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata2, *metaFather2, "Metadata in father does not match expected")

	metaLocal2, err := local.Db.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from local")
	assert.Equal(t, metadata2, *metaLocal2, "Metadata in local does not match expected")
}

// TestMetadataDeletionFromChildIntegration tests metadata deletion propagation from child to father
func TestMetadataDeletionFromChildIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	defer cleanupFather()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan", father)
	defer cleanupChild()

	geolocation := "IT/Milan"
	payload, err := json.Marshal(geolocation)
	require.NoError(t, err, "Expected no error marshaling geolocation to JSON")

	req, err := http.NewRequest(http.MethodDelete, "http://"+child.Server.ServerInfo.HostName+"/pvt-delete-metadata/"+collectionId2, bytes.NewBuffer(payload))
	require.NoError(t, err, "Expected no error creating HTTP request")
	req.Header.Set("X-ERMES-SECRET", secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "IT/Milan/test")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "Expected no error on HTTP request")
	res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected HTTP 200 OK response")

	// Verify that the metadata was deleted in both father and child
	metaChild, err := child.Db.GetMetadata(collectionId2)
	assert.Error(t, err, "Expected error when getting deleted metadata from child")
	assert.Nil(t, metaChild, "Metadata in child should be nil")

	metaFather, err := father.Db.GetMetadata(collectionId2)
	assert.Error(t, err, "Expected error when getting deleted metadata from father")
	assert.Nil(t, metaFather, "Metadata in father should be nil")
}

// TestMetadataDeletionFromFatherIntegration tests metadata deletion propagation from father to child
func TestMetadataDeletionFromFatherIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	father.Server.ServerInfo.Father = &server.Node{
		//root node, so that the father can think that the message arrive from top
	}
	defer cleanupFather()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan", father)
	defer cleanupChild()

	geolocation := "IT/Milan"
	jsonMetadata, err := json.Marshal(geolocation)
	require.NoError(t, err, "Expected no error marshaling geolocation to JSON")

	req, err := http.NewRequest(http.MethodDelete, "http://"+father.Server.ServerInfo.HostName+"/pvt-delete-metadata/"+collectionId2, bytes.NewBuffer(jsonMetadata))
	require.NoError(t, err, "Expected no error creating HTTP request")
	req.Header.Set("X-ERMES-SECRET", secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "Expected no error on HTTP request")
	res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected HTTP 200 OK response")

	// Verify that the metadata was created in both father and child
	metaChild, err := child.Db.GetMetadata(collectionId2)
	assert.Error(t, err, "Expected error when getting deleted metadata from child")
	assert.Nil(t, metaChild, "Metadata in child should be nil")

	metaFather, err := father.Db.GetMetadata(collectionId2)
	assert.Error(t, err, "Expected error when getting deleted metadata from father")
	assert.Nil(t, metaFather, "Metadata in father should be nil")
}

// TestMetadataDeletionFromLocalIntegration tests metadata deletion propagation from local node to both father and child
func TestMetadataDeletionFromLocalIntegration(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	// Topology: Father -> Local -> Child
	father, cleanupFather := setupMetadataNode(t, log.With("node", "father"), "IT", nil)
	defer cleanupFather()

	local, cleanupLocal := setupMetadataNode(t, log.With("node", "local"), "IT/Milan", father)
	defer cleanupLocal()

	child, cleanupChild := setupMetadataNode(t, log.With("node", "child"), "IT/Milan/Polimi", local)
	defer cleanupChild()

	// Test 1: Delete metadata with geolocation of local node
	metadata := getTestMetadata("test", "IT/Milan")
	err := local.MetadataManager.CreateMetadata(collectionId1, metadata, propagation.FROM_LOCAL)
	require.NoError(t, err, "Expected no error creating metadata from local node")

	err = local.MetadataManager.DeleteMetadata(collectionId1, metadata.Geolocation, propagation.FROM_LOCAL, false)
	assert.NoError(t, err, "Expected no error deleting metadata from local node")

	// Verify that the metadata was created in both father and child
	metaChild, err := child.Db.GetMetadata(collectionId1)
	assert.Error(t, err, "Expected error when getting deleted metadata from child")
	assert.Nil(t, metaChild, "Metadata in child should be nil")
	metaFather, err := father.Db.GetMetadata(collectionId1)
	assert.Error(t, err, "Expected error when getting deleted metadata from father")
	assert.Nil(t, metaFather, "Metadata in father should be nil")
	metaLocal, err := local.Db.GetMetadata(collectionId1)
	assert.Error(t, err, "Expected error when getting deleted metadata from local node")
	assert.Nil(t, metaLocal, "Metadata in local should be nil")

	// Test 2: Delete metadata with geolocation of child node
	metadata2 := getTestMetadata("test2", "IT/Milan/Polimi")
	err = local.MetadataManager.CreateMetadata("test2", metadata2, propagation.FROM_LOCAL)
	require.NoError(t, err, "Expected no error creating metadata from local node")

	err = local.MetadataManager.DeleteMetadata("test2", metadata2.Geolocation, propagation.FROM_LOCAL, false)
	assert.NoError(t, err, "Expected no error deleting metadata from local node")

	// Verify deletion everywhere
	metaChild, err = child.Db.GetMetadata("test2")
	assert.Error(t, err, err, "Expected error when getting deleted metadata from child")
	assert.Nil(t, metaChild, "Metadata in child should be nil")
	metaFather, err = father.Db.GetMetadata("test2")
	assert.Error(t, err, "Expected error when getting deleted metadata from father")
	assert.Nil(t, metaFather, "Metadata in father should be nil")
	metaLocal, err = local.Db.GetMetadata("test2")
	assert.Error(t, err, "Expected error when getting deleted metadata from local node")
	assert.Nil(t, metaLocal, "Metadata in local should be nil")
}
