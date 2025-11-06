//go:build test
// +build test

package tests

import (
	dataManager "dataManager"
	"dataManager/messages"
	database "db"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
)

func TestGetRecordLocally(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := DataManagerTestSetUp(t)

	dm := dataManager.GetDataManagerInstance()
	log := createNewLogger(t)
	dm.Init(collections, myT1PenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	cd := dm.GetCollectionsDiscoverer()
	defer dm.GetMigrationManager().StopAsyncDump()
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]db.RecordValue"), mock.AnythingOfType("[]string"), mock.AnythingOfType("string"), mock.AnythingOfType("uint64")).Return(nil).Times(2)
	fmt.Println("Inserting collections")

	//insert a strong collection
	dm.SetCollectionAndPropagate("strongRepoId", "strongRepoName", map[string]database.RecordValue{"record1": {Value: "value1"}, "record2": {Value: "value2"}}, []string{}, "", true, false)
	fmt.Println("Inserting collections 1 done")
	//insert a weak collection
	dm.SetCollectionAndPropagate("weakRepoId", "strongRepoName", map[string]database.RecordValue{"record1": {Value: "value1"}, "record2": {Value: "value2"}}, []string{}, "", false, false)
	fmt.Println("Inserting collections 2 done")

	//check strong collection
	bool, _, _ := cd.IsCollectionLocallyPresent("strongRepoId", 0)
	//check weak collection
	bool1, _, _ := cd.IsCollectionLocallyPresent("weakRepoId", 0)

	if !bool || !bool1 {
		t.Error("Expected collections to be present")
	}

	//setup mock for get record and get records from db
	db.On("IsAuthorized", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(true, nil).Times(2)
	db.On("GetRecord", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return("value1", nil).Times(1)
	db.On("GetRecords", mock.AnythingOfType("string"), []string{"record1", "record2"}).Return([]any{"value1", "value2"}, nil).Times(1)

	//get record from strong collection
	value, err := dm.GetRecord("strongRepoId", []string{}, "", "record1")
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if value != "value1" {
		t.Error("Unexpected value:", value)
	}

	//get records from weak collection
	records := []string{"record1", "record2"}

	values, err := dm.GetRecords("weakRepoId", []string{}, "", records...)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(values) != 2 {
		t.Error("Unexpected number of values:", len(values))
	}
	if values[0] != "value1" {
		t.Error("Unexpected value for record1:", values[0])
	}
	if values[1] != "value2" {
		t.Error("Unexpected value for record2:", values[1])
	}

	t.Cleanup(func() {
		dataManager.ResetSingleton()
	})
}

func TestGetRecordRemotelyToFather(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := DataManagerTestSetUp(t)

	dm := dataManager.GetDataManagerInstance()
	log := createNewLogger(t)
	dm.Init(collections, myT1PenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	//cd := dm.GetCollectionsDiscoverer()

	//set up assertions on db and server
	db.AssertNotCalled(t, "IsAuthorized", mock.Anything)
	db.AssertNotCalled(t, "GetRecord", mock.Anything, mock.Anything)
	db.AssertNotCalled(t, "GetRecords", mock.Anything, mock.Anything)

	var respBody messages.RemoteAccessResponse
	respBody.Records = []any{"value1"}
	respJSON, err := json.Marshal(respBody)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}

	ser.On("SendGenericHTTPRequestReturnResponse", mock.AnythingOfType("string"), "/pvt-dataRemoteAccess", mock.Anything).Return(&http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(respJSON))),
	}, nil).Times(1)

	dm.GetRecord("strongRepoId", []string{}, "", "record1")

	respBody.Records = []any{"value1", "value2"}
	respJSON, err = json.Marshal(respBody)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}
	ser.On("SendGenericHTTPRequestReturnResponse", mock.AnythingOfType("string"), "/pvt-dataRemoteAccess", mock.Anything).Return(&http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(respJSON))),
	}, nil).Times(1)

	dm.GetRecords("strongRepoId", []string{}, "record1", "record2")

	t.Cleanup(func() {
		dataManager.ResetSingleton()
	})
}

func TestUpdateCollectionsRecordsAndPropagate(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := DataManagerTestSetUp(t)

	dm := dataManager.GetDataManagerInstance()
	log := createNewLogger(t)
	dm.Init(collections, myT1PenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	cd := dm.GetCollectionsDiscoverer()
	defer dm.GetMigrationManager().StopAsyncDump()
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]db.RecordValue"), mock.AnythingOfType("[]string"), mock.AnythingOfType("string"), mock.AnythingOfType("uint64")).Return(nil).Times(1)
	ser.EXPECT().SendGenericHTTPRequest(mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything).Return(200, nil).Maybe()

	//insert a strong collection
	dm.SetCollectionAndPropagate("strongRepoId", "strongRepoName", map[string]database.RecordValue{"record1": {Value: "value1"}, "record2": {Value: "value2"}}, []string{}, "", true, false)

	//check strong collection
	bool, _, _ := cd.IsCollectionLocallyPresent("strongRepoId", 0)
	if !bool {
		t.Error("Expected collections to be present")
	}

	var update messages.CollectionUpdate = messages.CollectionUpdate{ //local update on leader
		CollectionId: "strongRepoId",
		RecordNames:  []string{"record1"},
		RecordValues: []string{"value1Updated"},
		IsDelete:     false,
	}
	var update1 messages.CollectionUpdate = messages.CollectionUpdate{ //remote update on father
		CollectionId: "weakRepoId",
		RecordNames:  []string{"record2"},
		RecordValues: []string{"value2Updated"},
		IsDelete:     false,
	}

	updates := []messages.CollectionUpdate{update, update1}

	db.On("IsAuthorized", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(true, nil).Times(1)
	db.On("SetRecords", "strongRepoId", []string{"record1"}, []string{"value1Updated"}, mock.AnythingOfType("string")).Return(nil).Times(1)

	err := dm.UpdateCollectionsRecordsAndPropagate(updates, []string{}, "")
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	t.Cleanup(func() {
		dataManager.ResetSingleton()
	})
}

func TestUpdateCollectionsGroupsAndPropagate(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := DataManagerTestSetUp(t)

	dm := dataManager.GetDataManagerInstance()
	log := createNewLogger(t)
	dm.Init(collections, myT1PenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	cd := dm.GetCollectionsDiscoverer()
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]db.RecordValue"), mock.AnythingOfType("[]string"), mock.AnythingOfType("string"), mock.AnythingOfType("uint64")).Return(nil).Times(1)
	ser.EXPECT().SendGenericHTTPRequest(mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything).Return(200, nil).Maybe()

	//insert a strong collection
	dm.SetCollectionAndPropagate("strongRepoId", "strongRepoName", map[string]database.RecordValue{"record1": {Value: "value1"}, "record2": {Value: "value2"}}, []string{}, "", true, false)

	//check strong collection
	bool, _, _ := cd.IsCollectionLocallyPresent("strongRepoId", 0)
	if !bool {
		t.Error("Expected collections to be present")
	}

	var update messages.CollectionGroupUpdate = messages.CollectionGroupUpdate{ //local update on leader
		CollectionId: "strongRepoId",
		GroupNames:   []string{"group1", "group2"},
		Sequence:     0,
	}
	var update1 messages.CollectionGroupUpdate = messages.CollectionGroupUpdate{ //remote update on father
		CollectionId: "weakRepoId",
		GroupNames:   []string{"group3", "group4"},
		Sequence:     0,
	}

	updates := []messages.CollectionGroupUpdate{update, update1}

	db.On("SetGroups", "strongRepoId", mock.Anything, mock.Anything).Return(nil).Times(1)

	err := dm.UpdateCollectionsGroupsAndPropagate(updates)

	if err != nil {
		t.Error("Unexpected error:", err)
	}

	t.Cleanup(func() {
		dataManager.ResetSingleton()
	})
}

func TestRemoveCollectionAndPropagate(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := DataManagerTestSetUp(t)

	dm := dataManager.GetDataManagerInstance()
	log := createNewLogger(t)
	dm.Init(collections, myT1PenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	cd := dm.GetCollectionsDiscoverer()
	defer dm.GetMigrationManager().StopAsyncDump()
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]db.RecordValue"), mock.AnythingOfType("[]string"), mock.AnythingOfType("string"), mock.AnythingOfType("uint64")).Return(nil).Times(2)
	ser.EXPECT().SendGenericHTTPRequest(mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything).Return(200, nil).Maybe()

	//insert a strong collection
	dm.SetCollectionAndPropagate("strongRepoId", "strongRepoName", map[string]database.RecordValue{"record1": {Value: "value1"}, "record2": {Value: "value2"}}, []string{}, "", true, false)
	//insert a weak collection
	dm.SetCollectionAndPropagate("weakRepoId", "weakRepoName", map[string]database.RecordValue{"record1": {Value: "value1"}, "record2": {Value: "value2"}}, []string{}, "", false, false)

	//check strong collection
	bool, _, _ := cd.IsCollectionLocallyPresent("strongRepoId", 0)
	//check weak collection
	bool1, _, _ := cd.IsCollectionLocallyPresent("weakRepoId", 0)

	if !bool || !bool1 {
		t.Error("Expected collections to be present")
	}

	//setup mock for get record and get records from db
	db.On("IsAuthorized", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(true, nil).Times(2)
	db.On("DeleteCollection", "strongRepoId").Return(nil).Times(1)
	db.On("DeleteCollection", "weakRepoId").Return(nil).Times(1)

	dm.RemoveCollectionAndPropagate("strongRepoId", []string{}, "")
	bool, _, _ = cd.IsCollectionLocallyPresent("strongRepoId", 0)

	if bool {
		t.Error("Expected collections not to be present")
	}

	dm.RemoveCollectionAndPropagate("weakRepoId", []string{}, "")
	bool1, _, _ = cd.IsCollectionLocallyPresent("weakRepoId", 0)

	if bool1 {
		t.Error("Expected collections not to be present")
	}

	t.Cleanup(func() {
		dataManager.ResetSingleton()
	})
}
