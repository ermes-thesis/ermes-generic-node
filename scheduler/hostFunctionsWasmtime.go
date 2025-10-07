package scheduler

import (
	"bytes"
	"dataManager/messages"
	shared "dataManager/shared"
	"errors"
	"fmt"
	"metadataManager"
	"net/http"

	"github.com/bytecodealliance/wasmtime-go/v37"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func RegisterWasmtimeHostFns(pool *wasm.WasmExecutorPool, dependencies HostFnsDependencies) error {
	fns := map[string]any{
		"hostGet": func(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeGet(collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostMultipleGet": func(collIdPtr int32, collIdLength int32, recordNamesPtr int32, recordNamesLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeMultipleGet(collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostCreateCollection": func(metadataPointer int32, metadataLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeCreateCollection(metadataPointer, metadataLength, &dependencies, caller)
		},
		"hostPut": func(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, valuePtr int32, valueLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimePut(collIdPtr, collIdLength, recordNamePtr, recordNameLength, valuePtr, valueLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostDelete": func(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeDelete(collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostDeleteCollection": func(collectionIdPtr int32, collectionIdLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeDeleteCollection(collectionIdPtr, collectionIdLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostAddGroupToCollection": func(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeAddGroupToCollection(collectionIdPtr, collectionIdLength, groupNamePtr, groupNameLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostRemoveGroupFromCollection": func(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeRemoveGroupFromCollection(collectionIdPtr, collectionIdLength, groupNamePtr, groupNameLength, groupsPtr, groupsLength, &dependencies, caller)
		},
		"hostAddUserToGroup": func(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeAddUserToGroup(userIdPtr, userIdLength, groupNamePtr, groupNameLength, &dependencies, caller)
		},
		"hostRemoveUserFromGroup": func(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeRemoveUserFromGroup(userIdPtr, userIdLength, groupNamePtr, groupNameLength, &dependencies, caller)
		},
		"hostCreateGroup": func(groupNamePtr int32, groupNameLength int32, partecipantIDsPtr int32, partecipantIDsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeCreateGroup(groupNamePtr, groupNameLength, partecipantIDsPtr, partecipantIDsLength, &dependencies, caller)
		},
		"hostDeleteGroup": func(groupNamePtr int32, groupNameLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeDeleteGroup(groupNamePtr, groupNameLength, &dependencies, caller)
		},
	}

	err := pool.RegisterHostFns(fns)
	if err != nil {
		return fmt.Errorf("failed to register module: %v", err)
	}
	return nil
}

func hostWasmtimeGet(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	// Read the result of the function.
	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	value, err := deps.DataManager.GetRecord(collId, groups, recordName)
	var ret int32
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			ret = wasmComponent.WriteError(model.Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			ret = wasmComponent.WriteError(model.NotFound, err)
		} else {
			ret = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		ret = wasmComponent.WriteResult(value)
	}

	return ret
}

func hostWasmtimeMultipleGet(collIdPtr int32, collIdLength int32, recordNamesPtr int32, recordNamesLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var recordNames []string
	err = wasmComponent.GetWasmObject(recordNamesPtr, recordNamesLength, &recordNames)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	values, err := deps.DataManager.GetRecords(collId, groups, recordNames...)
	var ret int32
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			ret = wasmComponent.WriteError(model.Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			ret = wasmComponent.WriteError(model.NotFound, err)
		} else {
			ret = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		ret = wasmComponent.WriteArrayResult(values)
	}

	return ret
}

func hostWasmtimePut(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, valuePtr int32, valueLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	value, err := wasmComponent.GetWasmString(valuePtr, valueLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	err = deps.DataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{{
		CollectionId: collId,
		RecordNames:  []string{recordName},
		RecordValues: []string{value},
		IsDelete:     false,
	}}, groups)
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			return wasmComponent.WriteError(model.Unauthorized, err)
		} else {
			return wasmComponent.WriteError(model.RedisError, err)
		}
	}

	return int32(model.Success)
}

func hostWasmtimeDelete(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	err = deps.DataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{{
		CollectionId: collId,
		RecordNames:  []string{recordName},
		IsDelete:     true,
	}}, groups)
	var ret int32
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			ret = wasmComponent.WriteError(model.Unauthorized, err)
		} else {
			ret = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		ret = int32(model.Success)
	}

	return ret
}

func hostWasmtimeCreateCollection(metadataPointer int32, metadataLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var metadata model.Metadata
	err := wasmComponent.GetWasmObject(metadataPointer, metadataLength, &metadata)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	groups := make([]any, len(metadata.Groups))
	for i, v := range metadata.Groups {
		groups[i] = v
	}

	collectionId := uuid.NewString()

	err = deps.DataManager.SetCollectionAndPropagate(collectionId, make(map[string]string), metadata.Groups, 0, metadata.Consistency == model.SequentialConsistent)
	if err != nil {
		return wasmComponent.WriteError(model.RedisError, err)
	}

	go deps.MetadataManager.CreateMetadata(collectionId, metadata, metadataManager.FROM_LOCAL)

	return wasmComponent.WriteResult(collectionId)
}

func hostWasmtimeDeleteCollection(collectionIdPtr int32, collectionIdLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	err = deps.DataManager.RemoveCollectionAndPropagate(collectionId, groups)

	//TODO: Delete the metadata only if the collection has been effectively deleted.

	return int32(model.Success)
}

func hostWasmtimeAddGroupToCollection(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var ret int32
	resp, err := deps.MetadataManager.AddGroupToCollection(collectionId, groups, groupName)
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			ret = wasmComponent.WriteError(model.Unauthorized, err)
		} else {
			ret = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		ret = int32(model.Success)
	}

	go func() {
		//TODO: chiamare DataManager per aggiornare il record speciale groups
		//err = deps.DataManager.SetCollectionGroups(collectionId, resp.Groups)
		_ = resp
	}()

	return ret
}

func hostWasmtimeRemoveGroupFromCollection(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var ret int32
	resp, err := deps.MetadataManager.RemoveGroupFromCollection(collectionId, groups, groupName)
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			ret = wasmComponent.WriteError(model.Unauthorized, err)
		} else {
			ret = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		ret = int32(model.Success)
	}

	go func() {
		//TODO: chiamare DataManager per aggiornare il record speciale groups
		//err = deps.DataManager.SetCollectionGroups(collectionId, resp.Groups)
		_ = resp
	}()

	return ret
}

func hostWasmtimeRemoveUserFromGroup(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusOK {
		ret = int32(model.Success)
	} else {
		ret = wasmComponent.WriteError(model.GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}

func hostWasmtimeAddUserToGroup(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusOK {
		ret = int32(model.Success)
	} else {
		ret = wasmComponent.WriteError(model.GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}

func hostWasmtimeCreateGroup(groupNamePtr int32, groupNameLength int32, partecipantIDsPtr int32, partecipantIDsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	participantIDsJsonStr, err := wasmComponent.GetWasmString(partecipantIDsPtr, partecipantIDsLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName, bytes.NewBufferString(participantIDsJsonStr))
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusCreated {
		ret = int32(model.Success)
	} else {
		ret = wasmComponent.WriteError(model.GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}

func hostWasmtimeDeleteGroup(groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := adapter.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName, nil)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(model.HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusOK {
		ret = int32(model.Success)
	} else {
		ret = wasmComponent.WriteError(model.GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}
