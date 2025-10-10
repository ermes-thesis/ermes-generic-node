package metadataManager_test

import (
	"encoding/json"
	"metadataManager"
	"metadataManager/types"
	"mocks/caches"
	"mocks/metadataManager"
	"server/info"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteQueries(t *testing.T) {

	redisClient, shutdownFun := startRedisContainer(t, false)
	defer shutdownFun()
	defer redisClient.Close()

	srvInfo := &info.ServerInfo{
		MetadataDomain: "IT",
	}

	requestedFnName := "test_function"
	requestedFnVersion := "1.0.0"
	queryTemplates := map[string]model.QueryTemplate{
		"private_data_query": {
			Select: "ALL",
			As:     "private_data",
			Where: &model.ConditionWrapper{Condition: &model.AndCondition{
				Conditions: []model.Condition{
					&model.EqCondition{
						Field: "owner",
						Value: "12345",
					},
					&model.EqCondition{
						Field: "name",
						Value: "<name>",
					},
				},
			}},
		},
		"gt_data_query": {
			Select: "ALL",
			As:     "gt_data",
			Where: &model.ConditionWrapper{Condition: &model.AndCondition{
				Conditions: []model.Condition{
					&model.EqCondition{
						Field: "owner",
						Value: "12345",
					},
					&model.EqCondition{
						Field: "name",
						Value: "DATI_PRIVATE",
					},
					&model.GtCondition{
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

	// Create a new MetadataManager instance
	metadataLog := createNewLogger(t)
	manager := metadataManager.NewMetadataManager(redisClient, srvInfo, metadataLog, fnCache)

	// Execute the query
	result, err := manager.ExecuteQueries(requestedFnName, requestedFnVersion, string(queryParams), []string{"groupC"}, "boh", "IT/Milan")
	assert.NoError(t, err, "Expected no error when executing queries")

	assert.Len(t, result, 1, "Expected result to be non-empty")

	hash := types.NewQueryIdentifier("private_data_query", map[string]string{
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
	redisClient, shutdownFun := startRedisContainer(t, false)
	defer shutdownFun()
	defer redisClient.Close()

	srvInfo := &info.ServerInfo{
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

	mPropagator := &metadataManager_mock.CreateMetadataPropagator{}
	mPropagator.EXPECT().Propagate("collection_id_test", metadata, metadataManager.FROM_LOCAL).Return(nil).Once()

	fnCache := &caches_mock.FunctionCache{}
	log := createNewLogger(t)
	manager := metadataManager.CreateMetadataManagerWithMocks(redisClient, srvInfo, log, nil, mPropagator, fnCache)

	// Execute the query
	err := manager.CreateMetadata("collection_id_test", metadata, metadataManager.FROM_LOCAL)
	require.NoError(t, err, "Expected no error when creating metadata")

	metaFromRedis, err := redisClient.GetMetadata("collection_id_test")
	require.NoError(t, err, "Expected no error when getting metadata from redis")

	require.Equal(t, metadata, *metaFromRedis, "Expected metadata to be equal to created metadata")
}
