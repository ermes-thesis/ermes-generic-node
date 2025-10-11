//go:build test
// +build test

package tests

import (
	dataManager "dataManager"
	messages "dataManager/messages"
	"encoding/json"
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
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]string"), mock.AnythingOfType("[]string"), mock.AnythingOfType("uint64")).Return(nil).Times(2)

	//insert a strong collection
	dm.SetCollectionAndPropagate("strongRepoId", map[string]string{"record1": "value1", "record2": "value2"}, []string{}, true)
	//insert a weak collection
	dm.SetCollectionAndPropagate("weakRepoId", map[string]string{"record1": "value1", "record2": "value2"}, []string{}, false)

	//check strong collection
	bool, _, _ := cd.IsCollectionLocallyPresent("strongRepoId", 0)
	//check weak collection
	bool1, _, _ := cd.IsCollectionLocallyPresent("weakRepoId", 0)

	if !bool || !bool1 {
		t.Error("Expected collections to be present")
	}

	//setup mock for get record and get records from db
	db.On("IsInGroups", mock.AnythingOfType("string")).Return(true, nil).Times(2)
	db.On("GetRecord", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return("value1", nil).Times(1)
	db.On("GetRecords", mock.AnythingOfType("string"), []string{"record1", "record2"}).Return([]any{"value1", "value2"}, nil).Times(1)

	//get record from strong collection
	value, err := dm.GetRecord("strongRepoId", []string{}, "record1")
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if value != "value1" {
		t.Error("Unexpected value:", value)
	}

	//get records from weak collection
	records := []string{"record1", "record2"}

	values, err := dm.GetRecords("weakRepoId", []string{}, records...)
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
}

func TestGetRecordRemotelyToFather(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := DataManagerTestSetUp(t)

	dm := dataManager.GetDataManagerInstance()
	log := createNewLogger(t)
	dm.Init(collections, myT1PenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	//cd := dm.GetCollectionsDiscoverer()

	//set up assertions on db and server
	db.AssertNotCalled(t, "IsInGroups", mock.Anything)
	db.AssertNotCalled(t, "GetRecord", mock.Anything, mock.Anything)
	db.AssertNotCalled(t, "GetRecords", mock.Anything, mock.Anything)

	var respBody messages.RemoteAccessResponse
	respBody.Records = []any{"value1"}
	respJSON, err := json.Marshal(respBody)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}

	ser.On("SendGenericHTTPRequestReturnResponse", mock.AnythingOfType("string"), "/dataRemoteAccess", mock.Anything).Return(&http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(respJSON))),
	}, nil).Times(1)

	dm.GetRecord("strongRepoId", []string{}, "record1")

	respBody.Records = []any{"value1", "value2"}
	respJSON, err = json.Marshal(respBody)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}
	ser.On("SendGenericHTTPRequestReturnResponse", mock.AnythingOfType("string"), "/dataRemoteAccess", mock.Anything).Return(&http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(respJSON))),
	}, nil).Times(1)

	dm.GetRecords("strongRepoId", []string{}, "record1", "record2")
}
