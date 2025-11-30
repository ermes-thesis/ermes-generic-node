package metadataManager_test

import (
	"db"
	"db/test_utils"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Constants & Global Configuration ---

const (
	tokenString   = "JWTToken"
	secret        = "secret"
	ownerID       = "12345"
	collectionId1 = "12345678"
	collectionId2 = "54321"
	testFnName    = "test_function"
	testFnVer     = "1.0.0"
	groupC        = "groupC"
	userID        = "userId"
)

// Query Templates used in tests
var templates = map[string]model.QueryTemplate{
	"private_data_query": {
		Select: "ALL",
		As:     "private_data",
		Where: &model.ConditionWrapper{Condition: &model.AndCondition{
			Conditions: []model.Condition{
				&model.EqCondition{Field: "owner", Value: ownerID},
				&model.EqCondition{Field: "name", Value: "<name>"},
			},
		}},
	},
	"gt_data_query": {
		Select: "ALL",
		As:     "gt_data",
		Where: &model.ConditionWrapper{Condition: &model.AndCondition{
			Conditions: []model.Condition{
				&model.EqCondition{Field: "owner", Value: 12345},
				&model.EqCondition{Field: "name", Value: "DATI_PRIVATE"},
				&model.GtCondition{Field: "created_at", Value: 1745747999},
			},
		}},
	},
}

// createNewLogger creates a new logger for tests
func createNewLogger(t *testing.T) logger.Logger {
	lvl := logger.WarnLevel
	if testing.Verbose() {
		lvl = logger.DebugLevel
	}
	log, err := logger.NewLogger(logger.Config{
		Level:  lvl,
		Format: "text",
	})
	require.NoError(t, err)
	return log
}

// setupRedis initializes a Redis client with test data
func setupRedis(t *testing.T, log logger.Logger, isChild bool) (db.GenericDB, func()) {
	client, shutdownFunc := test_utils.StartRedisTestContainer(t, log)
	err := client.CreateIndex()
	assert.NoError(t, err)

	_ = client.SetMetadata(ownerID, model.Metadata{
		Name:        "collectionName",
		Owner:       ownerID,
		CreatedAt:   time.Unix(1745748000, 0),
		Groups:      []string{"groupA", "groupB", groupC},
		Consistency: model.EventualConsistent,
		Geolocation: "IT/Como",
	})

	_ = client.SetMetadata(collectionId2, model.Metadata{
		Name:        "collectionName2",
		Owner:       ownerID,
		CreatedAt:   time.Unix(745748000, 0),
		Groups:      []string{groupC},
		Consistency: model.EventualConsistent,
		Geolocation: "IT/Milan",
	})

	if !isChild {
		_ = client.SetMetadata("1111", model.Metadata{
			Name:        "collectionNameFather",
			Owner:       ownerID,
			CreatedAt:   time.Unix(1745749000, 0),
			Groups:      []string{groupC},
			Consistency: model.EventualConsistent,
			Geolocation: "IT",
		})
	}
	return client, shutdownFunc
}
