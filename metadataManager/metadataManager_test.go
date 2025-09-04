package metadataManager_test

import (
	caches_mock "caches/mocks"
	"db"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"metadataManager"
	metadataManager_mock "metadataManager/mocks"
	"metadataManager/utils"
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

	requestedFnName := "test_function"
	requestedFnVersion := "1.0.0"
	queryTemplates := map[string]utils.Query{
		"private_data_query": {
			Select: "ALL",
			As:     "private_data",
			Where: &utils.ConditionWrapper{Condition: &utils.AndCondition{
				Conditions: []utils.Condition{
					&utils.EqCondition{
						Field: "owner",
						Value: "12345",
					},
					&utils.EqCondition{
						Field: "name",
						Value: "<name>",
					},
				},
			}},
		},
		"gt_data_query": {
			Select: "ALL",
			As:     "gt_data",
			Where: &utils.ConditionWrapper{Condition: &utils.AndCondition{
				Conditions: []utils.Condition{
					&utils.EqCondition{
						Field: "owner",
						Value: "12345",
					},
					&utils.EqCondition{
						Field: "name",
						Value: "DATI_PRIVATE",
					},
					&utils.GtCondition{
						Field: "created_at",
						Value: 1745747999,
					},
				},
			}},
		},
	}

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
		"gt_data_query": {},
	})

	fnCache := &caches_mock.FunctionCache{}
	q := queryTemplates["private_data_query"]
	q2 := queryTemplates["gt_data_query"]
	fnCache.EXPECT().GetTemplate(requestedFnName, requestedFnVersion, "private_data_query").Return(&q, nil)
	fnCache.EXPECT().GetTemplate(requestedFnName, requestedFnVersion, "gt_data_query").Return(&q2, nil)
	tknDecoder := &metadataManager_mock.TokenDecoder{}
	tknDecoder.EXPECT().DecodeGroupToken("bohhh").Return([]string{"groupC"}, nil)
	// Create a new MetadataManager instance
	manager := metadataManager.NewMetadataManager(redisClient, srvInfo, logger, fnCache, tknDecoder)

	// Execute the query
	result, err := manager.ExecuteQueries(requestedFnName, requestedFnVersion, string(queryParams), "bohhh", "IT/Milan")
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

	fnCache := &caches_mock.FunctionCache{}
	manager := metadataManager.CreateMetadataManagerWithMocks(redisClient, srvInfo, logger, nil, mPropagator, fnCache)

	// Execute the query
	err := manager.CreateMetadata("collection_id_test", metadata, metadataManager.FROM_LOCAL)
	require.NoError(t, err, "Expected no error when creating metadata")

	metaFromRedis, err := redisClient.GetMetadata("collection_id_test")
	require.NoError(t, err, "Expected no error when getting metadata from redis")

	require.Equal(t, metadata, *metaFromRedis, "Expected metadata to be equal to created metadata")
}
