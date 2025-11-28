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
	"net/http/httptest"
	"net/url"
	"server"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationOppositeUpdatePropagationAndReplica(t *testing.T) {
	//TODO: enable test after fixing propagation in opposite direction
	t.Skip()
	logger := createNewLogger(t)
	defer logger.Close()

	migrationManagerConfig := config.MigrationManagerConfig{
		IdleTimeoutMs:                1000000000000000000,
		LeaderThreshold:              0,
		FollowerThreshold:            1,
		ViewSize:                     3,
		GravityConstant:              0.981,
		KnownCollectionsDumpInterval: time.Minute,
		StrongPolicy:                 "ERMESPOLICY",
		WeakPolicy:                   "ALWAYSCOPYPOLICY",
	}
	lurConfig := config.LruConfig{
		Size:      100000,
		SafeLevel: 0.8,
	}

	dbFather, shutdownFather := test_utils.StartRedisTestContainer(t, logger.Component("father/redis"))
	defer shutdownFather()
	serverInfoFather := &server.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
	}
	serverFather, _ := server.NewServer(logger, serverInfoFather)
	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	collectionsFather := &shared.CollectionPartialView{
		KnownCollections: map[string]shared.Collection{},
		Mutex:            sync.Mutex{},
	}
	lruCacheFather := lru.LRUCacheConstructor[shared.Collection](lurConfig.Size)
	dataManagerFather := datamanager.NewDataManagerForTest(collectionsFather, penality.LogPenalty(2), migrationManagerConfig, lurConfig, serverFather, *serverFather.ServerInfo, &lruCacheFather, dbFather, logger.With("node", "father"))
	err := dataManagerFather.SetCollectionAndPropagate("col1", "test", map[string]db.RecordValue{}, []string{}, "userId", false, false)
	require.NoError(t, err, "Expected no error when setting collection in father")

	dbChild, shutdownChild := test_utils.StartRedisTestContainer(t, logger.Component("child/redis"))
	defer shutdownChild()

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")
	serverInfoChild := &server.ServerInfo{
		Father: &server.Node{
			HostName: urlFather.Host,
		},
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
	}
	serverFather.ServerInfo.HostName = urlFather.Host
	serverChild, _ := server.NewServer(logger, serverInfoChild)
	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	collectionsChild := &shared.CollectionPartialView{
		KnownCollections: map[string]shared.Collection{},
		Mutex:            sync.Mutex{},
	}
	lruCacheChild := lru.LRUCacheConstructor[shared.Collection](lurConfig.Size)

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverChild.ServerInfo.HostName = urlChild.Host
	dataManagerChild := datamanager.NewDataManagerForTest(collectionsChild, penality.LogPenalty(2), migrationManagerConfig, lurConfig, serverChild, *serverChild.ServerInfo, &lruCacheChild, dbChild, logger.With("node", "child"))

	dataManagerFather.GetMigrationManager().UpdatesRequestsInfo([]messages.CollectionRequestInfo{
		{
			CollectionId: "col1",
		},
	}, urlChild.Host)

	dataManagerChild.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{
		{
			CollectionId: "col1",
			RecordNames:  []string{"record1"},
			RecordValues: []string{"value1"},
		},
	}, []string{}, "userId")

	time.Sleep(1 * time.Second) // wait for propagation

	recordFather, err := dbFather.GetRecord("col1", "record1")
	require.NoError(t, err, "Expected no error when getting record from father")
	require.Equal(t, "value1", recordFather, "Expected record value to be 'value1' in father after propagation")

	recordChild, err := dbChild.GetRecord("col1", "record1")
	require.NoError(t, err, "Expected no error when getting record from child")
	require.Equal(t, "value1", recordChild, "Expected record value to be 'value1' in child")
}
