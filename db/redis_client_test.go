package db

import (
	"config"
	"context"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"metadataManager/utils"
	"net/url"
	"testing"
	"time"
)

func TestRecord(t *testing.T) {
	redisClient, shutdownFunc1 := startRedisContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	exists, err := redisClient.RecordExists("collection1", "record1")
	require.NoError(t, err, "Expected no error checking record existence")
	require.False(t, exists, "Expected record to not exist initially")

	err = redisClient.SetRecord("collection1", "record1", "value1")
	require.NoError(t, err, "Expected no error setting record")

	value, err := redisClient.GetRecord("collection1", "record1")
	require.NoError(t, err, "Expected no error getting record")
	require.Equal(t, "value1", value, "Expected record value to be 'value1'")

	exists, err = redisClient.RecordExists("collection1", "record1")
	require.NoError(t, err, "Expected no error checking record existence")
	require.True(t, exists, "Expected record to exist after setting it")

	err = redisClient.SetRecord("collection1", "record2", "value2")
	require.NoError(t, err, "Expected no error setting record")

	values, err := redisClient.GetRecords("collection1", "record1", "record2")
	require.NoError(t, err, "Expected no error getting multiple records")
	require.Len(t, values, 2, "Expected two records to be returned")
	require.Equal(t, "value1", values[0], "Expected first record value to be 'value1'")
	require.Equal(t, "value2", values[1], "Expected second record value to be 'value2'")

	err = redisClient.DeleteRecords("collection1", "record1")
	require.NoError(t, err, "Expected no error deleting record")

	exists, err = redisClient.RecordExists("collection1", "record1")
	require.NoError(t, err, "Expected no error checking record existence after deletion")
	require.False(t, exists, "Expected record to not exist after deletion")

	exists, err = redisClient.RecordExists("collection1", "record2")
	require.NoError(t, err, "Expected no error checking record existence after deletion")
	require.True(t, exists, "Expected record to not exist after deletion")
}

func TestCollection(t *testing.T) {
	redisClient, shutdownFunc1 := startRedisContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	collectionId := "collection1"
	records := map[string]string{
		"record1": "value1",
		"record2": "value2",
	}
	groups := []string{"group1", "group2"}

	err := redisClient.SetCollection(collectionId, records, groups)
	require.NoError(t, err, "Expected no error setting collection")

	colRecords, colGroups, err := redisClient.GetCollection(collectionId)
	require.NoError(t, err, "Expected no error getting collection")
	require.Equal(t, records, colRecords, "Expected collection records to match")
	require.ElementsMatch(t, groups, colGroups, "Expected collection groups to match")

	exists, err := redisClient.CollectionExists(collectionId)
	require.NoError(t, err, "Expected no error checking collection existence")
	require.True(t, exists, "Expected collection to exist after setting it")

	err = redisClient.DeleteCollection(collectionId)
	require.NoError(t, err, "Expected no error deleting collection")

	exists, err = redisClient.CollectionExists(collectionId)
	require.NoError(t, err, "Expected no error checking collection existence after deletion")
	require.False(t, exists, "Expected collection to not exist after deletion")
}

func TestMetadata(t *testing.T) {
	redisClient, shutdownFunc1 := startRedisContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	collectionId := "collection1"
	metadata := Metadata{
		Owner:          "owner1",
		Name:           "name1",
		Groups:         []string{"group1"},
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Consistency:    EVENTUAL_CONSISTENT,
		CustomMetadata: map[string]string{},
	}

	err := redisClient.SetMetadata(collectionId, metadata)
	require.NoError(t, err, "Expected no error setting metadata")

	retrievedMetadata, err := redisClient.GetMetadata(collectionId)
	require.NoError(t, err, "Expected no error getting all metadata")
	require.Equal(t, metadata.Owner, retrievedMetadata.Owner, "Expected owner to match")
	require.Equal(t, metadata.Name, retrievedMetadata.Name, "Expected name to match")

	err = redisClient.DeleteMetadata(collectionId)
	require.NoError(t, err, "Expected no error deleting metadata")

	retrievedMetadata, err = redisClient.GetMetadata(collectionId)
	require.NoError(t, err, "Expected no error getting all metadata after deletion")
	require.Nil(t, retrievedMetadata, "Expected no metadata after deletion")
}

func TestGroup(t *testing.T) {
	redisClient, shutdownFunc1 := startRedisContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	collectionId := "collection1"
	members := []string{"member1", "member2"}

	err := redisClient.AddGroups(collectionId, members...)
	require.NoError(t, err, "Expected no error adding group members")

	isInGroup, err := redisClient.IsInGroups(collectionId, "member1", "member3")
	require.NoError(t, err, "Expected no error checking group membership")
	require.True(t, isInGroup, "Expected member1 to be in group")

	err = redisClient.RemoveGroups(collectionId, "member1")
	require.NoError(t, err, "Expected no error removing group member")

	isInGroup, err = redisClient.IsInGroups(collectionId, "member1")
	require.NoError(t, err, "Expected no error checking group membership after removal")
	require.False(t, isInGroup, "Expected member1 to not be in group after removal")

	isInGroup, err = redisClient.IsInGroups(collectionId, "member2")
	require.NoError(t, err, "Expected no error checking group membership after removal")
	require.True(t, isInGroup, "Expected member2 to still be in group after removing member1")
}

func TestIndex(t *testing.T) {
	redisClient, shutdownFunc1 := startRedisContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	err := redisClient.CreateIndex("unused_key")
	require.NoError(t, err, "Expected no error creating index")

	err = redisClient.AlterIndex("custom_key")
	require.NoError(t, err, "Expected no error altering index")

	collectionId := "collection1"
	metadata := Metadata{
		Owner:  "owner1",
		Name:   "name1",
		Groups: []string{"group1"},
		CustomMetadata: map[string]string{
			"custom_key": "owner1",
		},
	}
	err = redisClient.SetMetadata(collectionId, metadata)
	require.NoError(t, err, "Expected no error setting metadata")

	query := utils.Query{
		Select: "ALL",
		As:     "test",
		Where: &utils.ConditionWrapper{
			Condition: &utils.EqCondition{
				Field: "custom_key",
				Value: "owner1",
			},
		},
	}
	groups := []string{"group1"}
	results, err := redisClient.SearchOnMetadata(query, groups)
	require.NoError(t, err, "Expected no error searching on metadata")
	require.Len(t, results, 1, "Expected one result from search")
	require.Equal(t, collectionId, results[0].Id, "Expected result ID to match collection ID")
	require.Equal(t, metadata.Owner, results[0].Metadata.Owner, "Expected result owner to match metadata owner")
	require.Equal(t, metadata.Name, results[0].Metadata.Name, "Expected result name to match metadata name")
	require.ElementsMatch(t, metadata.Groups, results[0].Metadata.Groups, "Expected result groups to match metadata groups")
	require.Equal(t, metadata.CustomMetadata, results[0].Metadata.CustomMetadata, "Expected result custom metadata to match")

	err = redisClient.DropIndex()
	require.NoError(t, err, "Expected no error dropping index")
}

func startRedisContainer(t *testing.T) (GenericDB, func()) {
	ctx := context.Background()
	redisContainer, err := tcredis.Run(ctx,
		"redis/redis-stack:latest",
		tcredis.WithSnapshotting(10, 1),
		tcredis.WithLogLevel(tcredis.LogLevelVerbose),
		testcontainers.WithCmd("redis-stack-server"),
		//tcredis.WithConfigFile(filepath.Join("testdata", "redis7.conf")),
	)

	if err != nil {
		t.Fatalf("failed to start container: %s", err)
		return nil, nil
	}

	connectionString, _ := redisContainer.ConnectionString(ctx)
	parsedUrl, _ := url.Parse(connectionString)

	client := NewRedisClient(config.RedisConfig{
		Address:  parsedUrl.Hostname(),
		Port:     parsedUrl.Port(),
		Password: "",
		DB:       0,
		Protocol: 2,
	}, ctx)

	return client, func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
