package metadataManager_test

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"metadataManager"
	"os"
	"server/info"
	"testing"
)

func TestLookupManager(t *testing.T) {

	// Create a logger
	logger := os.Stdout

	redisClient, shutdownFun := startRedisContainer(t, false)
	defer shutdownFun()
	defer redisClient.Close()

	srvInfo := &info.ServerInfo{}

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
