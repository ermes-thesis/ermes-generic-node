package redis_test

import (
	"db"
	"db/test_utils"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	exists, err := redisClient.RecordExists("collection1", "record1")
	require.NoError(t, err, "Expected no error checking record existence")
	require.False(t, exists, "Expected record to not exist initially")

	err = redisClient.SetRecord("collection1", "record1", "value1", "")
	require.EqualError(t, err, "collection does not exist")

	err = redisClient.SetCollection("collection1", nil, nil, "", 0)
	require.NoError(t, err, "Expected no error setting collection")

	err = redisClient.SetRecord("collection1", "record1", "value1", "")
	require.NoError(t, err, "Expected no error setting record")

	value, err := redisClient.GetRecord("collection1", "record1")
	require.NoError(t, err, "Expected no error getting record")
	require.Equal(t, "value1", value, "Expected record value to be 'value1'")

	exists, err = redisClient.RecordExists("collection1", "record1")
	require.NoError(t, err, "Expected no error checking record existence")
	require.True(t, exists, "Expected record to exist after setting it")

	err = redisClient.SetRecord("collection1", "record2", "value2", "")
	require.NoError(t, err, "Expected no error setting record")

	values, err := redisClient.GetRecords("collection1", "record1", "record2")
	require.NoError(t, err, "Expected no error getting multiple records")
	require.Len(t, values, 2, "Expected two records to be returned")
	require.Equal(t, "value1", values[0], "Expected first record value to be 'value1'")
	require.Equal(t, "value2", values[1], "Expected second record value to be 'value2'")

	err = redisClient.DeleteRecords("collection1", "", "record1")
	require.NoError(t, err, "Expected no error deleting record")

	exists, err = redisClient.RecordExists("collection1", "record1")
	require.NoError(t, err, "Expected no error checking record existence after deletion")
	require.False(t, exists, "Expected record to not exist after deletion")

	exists, err = redisClient.RecordExists("collection1", "record2")
	require.NoError(t, err, "Expected no error checking record existence after deletion")
	require.True(t, exists, "Expected record to not exist after deletion")

	err = redisClient.SetRecords("collection1", []string{"record10", "record11", "record12", "record13"}, []string{"value10", "value11", "value12"}, "")
	require.Error(t, err, "Expected no error setting record")

	err = redisClient.SetRecords("collection1", []string{"record10", "record11", "record12", "record13"}, []string{"value10", "value11", "value12", "value13"}, "")
	require.NoError(t, err, "Expected no error setting record")
	values2, err := redisClient.GetRecords("collection1", "record10", "record11", "record12", "record13")
	require.NoError(t, err, "Expected no error getting multiple records")
	require.Len(t, values2, 4, "Expected two records to be returned")
	require.Equal(t, "value10", values2[0], "Expected first record value to be 'value10'")
	require.Equal(t, "value11", values2[1], "Expected second record value to be 'value11'")
	require.Equal(t, "value12", values2[2], "Expected third record value to be 'value12'")
	require.Equal(t, "value13", values2[3], "Expected fourth record value to be 'value13'")
}

func TestCollection(t *testing.T) {
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	collectionId := "collection1"
	records := map[string]db.RecordValue{
		"record1": {Value: "value1"},
		"record2": {Value: "value1"},
	}
	groups := []string{"group1", "group2"}
	owner := "test_owner"

	err := redisClient.SetCollection(collectionId, records, groups, owner, 0)
	require.NoError(t, err, "Expected no error setting collection")

	colRecords, colGroups, colOwner, groupSequence, err := redisClient.GetCollection(collectionId)
	require.NoError(t, err, "Expected no error getting collection")
	require.Equal(t, records, colRecords, "Expected collection records to match")
	require.Equal(t, uint64(0), groupSequence, "Expected group sequence to be 0")
	require.ElementsMatch(t, groups, colGroups, "Expected collection groups to match")
	require.Equal(t, owner, colOwner, "Expected collection owners to match")

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
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	collectionId := "collection1"
	metadata := model.Metadata{
		Owner:          "owner1",
		Name:           "name1",
		Groups:         []string{"group1"},
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Consistency:    model.EventualConsistent,
		CustomMetadata: map[string]string{},
	}

	err := redisClient.SetMetadata(collectionId, metadata)
	require.NoError(t, err, "Expected no error setting metadata")

	retrievedMetadata, err := redisClient.GetMetadata(collectionId)
	require.NoError(t, err, "Expected no error getting all metadata")
	require.Equal(t, metadata.Owner, retrievedMetadata.Owner, "Expected owner to match")
	require.Equal(t, metadata.Name, retrievedMetadata.Name, "Expected name to match")
	require.Equal(t, metadata, *retrievedMetadata, "Expected metadata to match")

	err = redisClient.DeleteMetadata(collectionId, false)
	require.NoError(t, err, "Expected no error deleting metadata")

	retrievedMetadata, err = redisClient.GetMetadata(collectionId)
	require.Error(t, err, "Expected error getting metadata after deletion")
	require.Nil(t, retrievedMetadata, "Expected no metadata after deletion")
}

func TestGroup(t *testing.T) {
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	collectionId := "collection1"
	members := []string{"member1", "member2"}

	err := redisClient.SetCollection(collectionId, nil, nil, "coll_owner", 0)
	require.NoError(t, err, "Expected no error setting collection")

	err = redisClient.SetMetadata(collectionId, model.Metadata{
		Name:           "collection_name",
		Owner:          "coll_owner",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{},
		Consistency:    model.EventualConsistent,
		Geolocation:    "IT/Milan",
		CustomMetadata: nil,
	})
	require.NoError(t, err, "Expected no error setting collection")

	groups, groupsSequence, err := redisClient.UpdateGroupsInMetadata(collectionId, members, []string{})
	require.NoError(t, err, "Expected no error adding group members")
	require.Equal(t, members, groups, "Expected group members to match")
	require.Equal(t, uint64(1), groupsSequence, "Expected group sequence to be 1 after first addition")

	isInGroup, err := redisClient.IsAuthorizedInMetadata(collectionId, "", "member1", "member3")
	require.NoError(t, err, "Expected no error checking group membership")
	require.True(t, isInGroup, "Expected member1 to be in group")

	err = redisClient.SetGroups(collectionId, groupsSequence, groups...)
	require.NoError(t, err, "Expected no error setting groups with correct sequence")

	isInGroup, err = redisClient.IsAuthorized(collectionId, "", "member1", "member3")
	require.NoError(t, err, "Expected no error checking group membership")
	require.True(t, isInGroup, "Expected member1 to be in group")

	groups, groupsSequence, err = redisClient.UpdateGroupsInMetadata(collectionId, []string{}, []string{"member1"})
	require.NoError(t, err, "Expected no error removing group member")
	require.Equal(t, []string{"member2"}, groups, "Expected remaining group members to match")
	require.Equal(t, uint64(2), groupsSequence, "Expected group sequence to be 2 after removal")

	isInGroup, err = redisClient.IsAuthorizedInMetadata(collectionId, "", "member1")
	require.NoError(t, err, "Expected no error checking group membership after removal")
	require.False(t, isInGroup, "Expected member1 to not be in group after removal")

	isInGroup, err = redisClient.IsAuthorizedInMetadata(collectionId, "", "member2")
	require.NoError(t, err, "Expected no error checking group membership after removal")
	require.True(t, isInGroup, "Expected member2 to still be in group after removing member1")

	err = redisClient.SetGroups(collectionId, groupsSequence, groups...)
	require.NoError(t, err, "Expected no error setting groups with correct sequence")

	isInGroup, err = redisClient.IsAuthorized(collectionId, "", "member1")
	require.NoError(t, err, "Expected no error checking group membership after removal")
	require.False(t, isInGroup, "Expected member1 to not be in group after removal")

	isInGroup, err = redisClient.IsAuthorized(collectionId, "", "member2")
	require.NoError(t, err, "Expected no error checking group membership after removal")
	require.True(t, isInGroup, "Expected member2 to still be in group after removing member1")
}

func TestIndex(t *testing.T) {
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	err := redisClient.CreateIndex("unused_key")
	require.NoError(t, err, "Expected no error creating index")

	err = redisClient.SetMetadata("collId", model.Metadata{
		Name:           "TestMetadata",
		Owner:          "owner1",
		Groups:         []string{"group1"},
		CustomMetadata: map[string]string{"unused_key": "value_unused"},
	})
	require.NoError(t, err, "Expected no error setting metadata")

	err = redisClient.AlterIndex("custom_key")
	require.NoError(t, err, "Expected no error altering index")

	collectionId := "collection1"
	metadata := model.Metadata{
		Owner:  "owner1",
		Name:   "name1",
		Groups: []string{"group1"},
		CustomMetadata: map[string]string{
			"custom_key": "owner1",
		},
	}
	err = redisClient.SetMetadata(collectionId, metadata)
	require.NoError(t, err, "Expected no error setting metadata")

	query := model.QueryTemplate{
		Select: "ALL",
		As:     "test",
		Where: &model.ConditionWrapper{
			Condition: &model.EqCondition{
				Field: "custom_key",
				Value: "owner1",
			},
		},
	}
	query2 := model.QueryTemplate{
		Select: "ALL",
		As:     "test2",
		Where: &model.ConditionWrapper{
			Condition: &model.EqCondition{
				Field: "unused_key",
				Value: "value_unused",
			},
		},
	}

	groups := []string{"group1"}
	results, err := redisClient.SearchOnMetadata(query, "", groups)
	require.NoError(t, err, "Expected no error searching on metadata")
	require.Len(t, results, 1, "Expected one result from search")
	require.Equal(t, collectionId, results[0].Id, "Expected result ID to match collection ID")
	require.Equal(t, metadata.Owner, results[0].Metadata.Owner, "Expected result owner to match metadata owner")
	require.Equal(t, metadata.Name, results[0].Metadata.Name, "Expected result name to match metadata name")
	require.ElementsMatch(t, metadata.Groups, results[0].Metadata.Groups, "Expected result groups to match metadata groups")
	require.Equal(t, metadata.CustomMetadata, results[0].Metadata.CustomMetadata, "Expected result custom metadata to match")

	// Check if collection is retrieved also with only the owner
	results, err = redisClient.SearchOnMetadata(query, "owner1", nil)
	require.NoError(t, err, "Expected no error searching on metadata")
	require.Len(t, results, 1, "Expected one result from search")
	require.Equal(t, collectionId, results[0].Id, "Expected result ID to match collection ID")
	require.Equal(t, metadata.Owner, results[0].Metadata.Owner, "Expected result owner to match metadata owner")
	require.Equal(t, metadata.Name, results[0].Metadata.Name, "Expected result name to match metadata name")
	require.ElementsMatch(t, metadata.Groups, results[0].Metadata.Groups, "Expected result groups to match metadata groups")
	require.Equal(t, metadata.CustomMetadata, results[0].Metadata.CustomMetadata, "Expected result custom metadata to match")

	results, err = redisClient.SearchOnMetadata(query2, "owner1", groups)
	require.NoError(t, err, "Expected no error searching on metadata")
	require.Len(t, results, 1, "Expected one result from search")
	require.Equal(t, "collId", results[0].Id, "Expected result ID to match collection ID")
	require.Equal(t, "owner1", results[0].Metadata.Owner, "Expected result owner to match metadata owner")
	require.Equal(t, "TestMetadata", results[0].Metadata.Name, "Expected result name to match metadata name")
	require.ElementsMatch(t, []string{"group1"}, results[0].Metadata.Groups, "Expected result groups to match metadata groups")
	require.Equal(t, map[string]string{"unused_key": "value_unused"}, results[0].Metadata.CustomMetadata, "Expected result custom metadata to match")

	err = redisClient.DropIndex()
	require.NoError(t, err, "Expected no error dropping index")
}

func TestMemStats(t *testing.T) {
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	emptyStats, err := redisClient.GetMemoryStats()
	require.NoError(t, err, "Expected no error getting memory stats on empty DB")
	require.Equal(t, uint64(0), emptyStats.KeysCount, "Expected 0 keys in empty DB")

	collectionId := "collection1"
	groups := []string{"group1", "group2"}

	maxMem, err := redisClient.GetMaxMemory()
	require.NoError(t, err, "Expected no error getting max memory")
	require.Equal(t, uint64(0x1f400000), maxMem, "Expected max memory to be 500mb")
	err = redisClient.SetMetadata(collectionId, model.Metadata{
		Owner:          "owner1",
		Name:           "name1",
		Groups:         groups,
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Geolocation:    "IT",
		Consistency:    model.EventualConsistent,
		CustomMetadata: map[string]string{},
	})
	require.NoError(t, err, "Expected no error setting metadata")

	err = redisClient.SetCollection(collectionId, map[string]db.RecordValue{
		"record1":  {Value: "value1"},
		"record2":  {Value: "value2"},
		"record3":  {Value: "value3"},
		"record4":  {Value: "value4"},
		"record5":  {Value: "value5"},
		"record6":  {Value: "value6"},
		"record7":  {Value: "value7"},
		"record8":  {Value: "value8"},
		"record9":  {Value: "value9"},
		"record10": {Value: "value10"},
	}, groups, "", 0)
	require.NoError(t, err, "Expected no error setting collection metadata")

	// Wait a second to ensure Redis has updated its memory stats
	time.Sleep(1 * time.Second)

	memStats1, err := redisClient.GetMemoryStats()
	require.NoError(t, err, "Expected no error getting memory stats")

	require.Greater(t, memStats1.UsedMemoryDatasetPercentage, float64(0), "Expected UsedMemoryDatasetPercentage to be greater than 0")
	// 10 "Normal record" + 10 timestamp records + 1 metadata record + 5 special records
	require.Equal(t, uint64(1+20+5), memStats1.KeysCount)
	require.LessOrEqual(t, memStats1.CurrentUsedMemory, memStats1.UsedMemoryPeak)
	require.Equal(t, memStats1.UsedMemoryDataset/memStats1.KeysCount, memStats1.BytesKeysRatio)
	require.Equal(t, uint64(0xc0), memStats1.LuaScriptsCacheSize)
	require.Equal(t, uint64(0), memStats1.ReplicationBacklogSize)
	require.Greater(t, memStats1.UsedMemoryOverhead, uint64(0))
	require.Greater(t, memStats1.UsedMemoryStartup, uint64(0))

	collectionId2 := "collection2"
	err = redisClient.SetCollection(collectionId2, map[string]db.RecordValue{
		"record11": {Value: "value11"},
		"record12": {Value: "value12"},
		"record13": {Value: "value13"},
		"record14": {Value: "value14"},
		"record15": {Value: "value15"},
	}, groups, "", 0)
	require.NoError(t, err, "Expected no error setting collection metadata")

	// Wait a second to ensure Redis has updated its memory stats
	time.Sleep(1 * time.Second)

	var memStats2 *db.MemStats
	require.Eventually(t, func() bool {
		memStats2, err = redisClient.GetMemoryStats()
		require.NoError(t, err, "Expected no error getting memory stats")
		available, err := redisClient.GetAvailableMemory()
		require.NoError(t, err, "Expected no error getting available memory")
		return maxMem-memStats2.CurrentUsedMemory == available
	}, 3*time.Second, 100*time.Millisecond)

	// 15 "Normal record" + 15 timestamp records + 1 metadata record + 5 special records for each collection (that are 2)
	require.Equal(t, memStats2.KeysCount, uint64(30+1+5*2))
	require.LessOrEqual(t, memStats2.CurrentUsedMemory, memStats2.UsedMemoryPeak)
	require.Equal(t, memStats2.BytesKeysRatio, memStats2.UsedMemoryDataset/memStats2.KeysCount)
	require.Equal(t, memStats2.LuaScriptsCacheSize, uint64(0xc0))
	require.Equal(t, memStats2.ReplicationBacklogSize, uint64(0))
	require.Greater(t, memStats2.UsedMemoryOverhead, uint64(0))
	require.Greater(t, memStats2.UsedMemoryStartup, uint64(0))

	require.Greater(t, memStats2.UsedMemoryOverhead, memStats1.UsedMemoryOverhead)
	require.Equal(t, memStats2.UsedMemoryStartup, memStats1.UsedMemoryStartup)
	require.Greater(t, memStats2.UsedMemoryDataset, memStats1.UsedMemoryDataset)
	require.Greater(t, memStats2.KeysCount, memStats1.KeysCount)

	collSize, err := redisClient.GetCollectionMemoryUsage(collectionId)
	require.NoError(t, err, "Expected no error getting collection memory usage")
	require.Greater(t, collSize, uint64(0), "Expected collection memory usage to not be zero")

	err = redisClient.SetRecord(collectionId, "recordTest", "valueTest", "")
	require.NoError(t, err, "Expected no error setting record")

	collSize2, err := redisClient.GetCollectionMemoryUsage(collectionId)
	require.NoError(t, err, "Expected no error getting collection memory usage")
	require.Greater(t, collSize2, collSize, "Expected collection memory usage to not be zero")

	recordSize, err := redisClient.GetRecordMemoryUsage(collectionId, "recordTest")
	require.NoError(t, err, "Expected no error getting record")
	require.Greater(t, recordSize, uint64(0), "Expected recordMemoryUsage to not be zero")

	err = redisClient.SetRecord(collectionId, "recordTest", "Record with higher value size", "")
	require.NoError(t, err, "Expected no error setting record")

	recordSize2, err := redisClient.GetRecordMemoryUsage(collectionId, "recordTest")
	require.NoError(t, err, "Expected no error getting record")
	require.Greater(t, recordSize2, recordSize, "Expected recordMemoryUsage to not be zero")
}

func TestMemoryStress(t *testing.T) {
	redisClient, shutdownFunc1 := test_utils.StartRedisTestContainer(t)
	defer shutdownFunc1()
	defer redisClient.Close()

	err := redisClient.SetCollection("collection1", nil, nil, "", 0)
	fmt.Println("Starting...")
	for i := 0; i < 1000; i++ {
		names := make([]string, 1000)
		val := make([]string, 1000)
		for j := 0; j < 1000; j++ {
			names[j] = "record" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			val[j] = "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue" + strconv.Itoa(i)
		}
		err := redisClient.SetRecords("collection1", names, val, "")
		require.NoError(t, err, "Expected no error setting record")
		if i%10 == 0 {
			fmt.Println("10.000 done, still going!!")
		}
	}

	res, err := redisClient.GetMemoryStats()
	fmt.Printf("Mem stats: %+v\n", res)
	require.NoError(t, err, "Expected no error getting memory stats")
	require.Exactly(t, uint64(2_000_004), res.KeysCount, "Expected 2000003 keys in memory stats")
}
