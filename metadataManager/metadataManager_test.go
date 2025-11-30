package metadataManager_test

import (
	"encoding/json"
	"metadataManager"
	"metadataManager/propagation"
	"mocks/caches"
	"mocks/metadataManager/propagation"
	"server"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteQueries(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	redisClient, shutdownFun := setupRedis(t, log.Component("redis"), false)
	defer shutdownFun()

	srvInfo := &server.ServerInfo{
		MetadataDomain: "IT",
	}

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
		"gt_data_query": {},
	})

	fnCache := caches_mock.NewFunctionCache(t)
	q := templates["private_data_query"]
	q2 := templates["gt_data_query"]
	fnCache.EXPECT().GetTemplate(testFnName, testFnVer, "private_data_query").Return(&q, nil)
	fnCache.EXPECT().GetTemplate(testFnName, testFnVer, "gt_data_query").Return(&q2, nil)

	// Create a new MetadataManager instance
	manager, err := metadataManager.NewMetadataManager(redisClient, srvInfo, log.Component("metadata_manager"), fnCache)
	assert.NoError(t, err, "Expected no error when creating metadata manager")

	// Execute the query
	result, err := manager.ExecuteQueries(testFnName, testFnVer, string(queryParams), []string{"groupC"}, "12345", "boh", "IT/Milan")
	assert.NoError(t, err, "Expected no error when executing queries")

	assert.Len(t, result, 1, "Expected result to be non-empty")

	hash := model.NewQueryIdentifier("private_data_query", map[string]string{
		"name": "collectionNameFather",
	}).Hash()
	assert.Contains(t, result, hash, "Expected result to contain 'private_data' key")

	assert.Equal(t, result[hash].As, "private_data", "Expected 'As' field to be 'private_data'")
	assert.Len(t, result[hash].Collections, 1, "Expected 'private_data' to have one collection")
	collection := result[hash].Collections[0]

	assert.Equal(t, "1111", collection.Id, "Expected collection ID to be 1111")
	assert.Equal(t, "12345", collection.Metadata.Owner, "Expected owner to match query parameter")
	assert.Equal(t, "collectionNameFather", collection.Metadata.Name, "Expected name to be 'collectionNameFather'")
}

func TestCreateMetadata(t *testing.T) {
	log := createNewLogger(t)
	defer log.Close()

	redisClient, shutdownFun := setupRedis(t, log.Component("redis"), false)
	defer shutdownFun()

	srvInfo := &server.ServerInfo{
		MetadataDomain: "IT",
	}

	tm := time.Now().Truncate(time.Millisecond)
	metadata := model.Metadata{
		Name:        "test",
		Owner:       "fake",
		CreatedAt:   tm,
		Groups:      []string{"g1", "g2", "g3"},
		Consistency: model.EventualConsistent,
		Geolocation: "IT",
		CustomMetadata: map[string]string{
			"key1": "value1",
		},
	}

	mPropagator := propagation_mock.NewCreateMetadataPropagator(t)
	mPropagator.EXPECT().Propagate("collection_id_test", metadata, propagation.FROM_LOCAL).Return(nil).Once()

	fnCache := caches_mock.NewFunctionCache(t)
	manager := metadataManager.CreateMetadataManagerWithMocks(redisClient, srvInfo, log.Component("metadata_manager"), nil, mPropagator, fnCache)

	// Execute the query
	err := manager.CreateMetadata("collection_id_test", metadata, propagation.FROM_LOCAL)
	require.NoError(t, err, "Expected no error when creating metadata")

	metaFromRedis, err := redisClient.GetMetadata("collection_id_test")
	require.NoError(t, err, "Expected no error when getting metadata from redis")

	require.Equal(t, metadata, *metaFromRedis, "Expected metadata to be equal to created metadata")
}
