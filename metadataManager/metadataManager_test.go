package metadataManager_test

import (
	"db"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"metadataManager"
	metadataManager_mock "metadataManager/mocks"
	"os"
	"server/info"
	"testing"
	"time"
)

func TestExecuteQueries(t *testing.T) {
	// Create a logger
	logger := os.Stdout

	redisClient, shutdownFun := startRedisContainer(t, false)
	defer shutdownFun()
	defer redisClient.Close()

	srvInfo := &info.ServerInfo{
		Node: info.Node{
			MetadataDomain: "IT",
		},
	}

	// Create a new MetadataManager instance
	manager := metadataManager.NewMetadataManager(redisClient, srvInfo, logger)

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
		"gt_data_query": {},
	})

	// Execute the query
	result, err := manager.ExecuteQueries(string(queryParams), "bohhh", "IT/Milan")
	assert.NoError(t, err, "Expected no error when executing queries")

	assert.Len(t, result, 1, "Expected result to be non-empty")
	assert.Contains(t, result, "private_data", "Expected result to contain 'private_data' key")

	assert.Len(t, result["private_data"], 1, "Expected 'private_data' to have one collection")
	collection := result["private_data"][0]

	assert.Equal(t, "1111", collection.Id, "Expected collection ID to be 1111")
	assert.Equal(t, "12345", collection.Metadata.Owner, "Expected owner to match query parameter")
	assert.Equal(t, "collectionNameFather", collection.Metadata.Name, "Expected name to be 'collectionNameFather'")
}

func TestCreateMetadata(t *testing.T) {
	// Create a logger
	logger := os.Stdout

	redisClient, shutdownFun := startRedisContainer(t, false)
	defer shutdownFun()
	defer redisClient.Close()

	srvInfo := &info.ServerInfo{
		Node: info.Node{
			MetadataDomain: "IT",
		},
	}

	tm := time.Now().Truncate(time.Millisecond)
	metadata := db.Metadata{
		Name:        "test",
		Owner:       "fake",
		CreatedAt:   tm,
		Groups:      []string{"g1", "g2", "g3"},
		Consistency: db.EVENTUAL_CONSISTENT,
		Geolocation: "IT",
		CustomMetadata: map[string]string{
			"key1": "value1",
		},
	}

	mPropagator := &metadataManager_mock.MetadataPropagator{}
	mPropagator.EXPECT().Propagate("collection_id_test", metadata, metadataManager.FROM_LOCAL).Return(nil).Once()

	manager := metadataManager.CreateMetadataManagerWithMocks(redisClient, srvInfo, logger, nil, mPropagator)

	// Execute the query
	err := manager.CreateMetadata("collection_id_test", metadata, metadataManager.FROM_LOCAL)
	require.NoError(t, err, "Expected no error when creating metadata")

	metaFromRedis, err := redisClient.GetMetadata("collection_id_test")
	require.NoError(t, err, "Expected no error when getting metadata from redis")

	require.Equal(t, metadata, *metaFromRedis, "Expected metadata to be equal to created metadata")
}
