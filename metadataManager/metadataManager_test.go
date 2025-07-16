package metadataManager

import (
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"os"
	"server/info"
	"testing"
)

func TestLookupManager(t *testing.T) {

	// Create a logger
	logger := os.Stdout

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81",
		DB:       0,
		Protocol: 2,
	})

	srvInfo := &info.Serverinfo{}

	// Create a new MetadataManager instance
	manager := NewMetadataManager(rdb, srvInfo, logger)

	queryParams := map[string]map[string]any{
		"private_data_query": {
			"ownerID": "12345",
		},
		"gt_data_query": {},
	}

	// Execute the query
	groups := []string{"groupA", "groupB", "groupC"}
	result, err := manager.ExecuteQueries(queryParams, groups)
	assert.NoError(t, err, "Expected no error when executing queries")

	assert.NotEmpty(t, result, "Expected result to be non-empty")
	assert.Equal(t, 2, len(result), "Expected result to be non-empty")
	assert.Contains(t, result, "private_data", "Expected result to contain 'private_data' key")
	assert.NotEmpty(t, result["private_data"], "Expected 'private_data' to have non-empty results")
	for _, collection := range result["private_data"] {
		assert.Equal(t, "12345", collection.Id, "Expected collection ID to be 12345")
		assert.NotEmpty(t, collection.Metadata, "Expected collection metadata to be non-empty")
		assert.Contains(t, collection.Metadata, "owner", "Expected metadata to contain 'owner' key")
		assert.Equal(t, collection.Metadata["owner"], "12345", "Expected owner to match query parameter")
		assert.Contains(t, collection.Metadata, "name", "Expected metadata to contain 'name' key")
		assert.Equal(t, collection.Metadata["name"], "DATI_PRIVATE", "Expected name to be 'DATI_PRIVATE'")
	}
}
