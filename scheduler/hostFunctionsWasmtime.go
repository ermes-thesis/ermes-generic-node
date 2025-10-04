package scheduler

import (
	"bytes"
	shared "dataManager/shared"
	"errors"
	"fmt"
	"github.com/bytecodealliance/wasmtime-go/v37"
	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	wasm_utils "github.com/ermes-thesis/ermes-wasm-pool/wasm-pool/executor/utils"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"server/client"
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
			return hostWasmtimeAddGroupToCollection(collectionIdPtr, collectionIdLength, groupsPtr, groupNamePtr, groupNameLength, groupsLength, &dependencies, caller)
		},
		"hostRemoveGroupFromCollection": func(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return hostWasmtimeRemoveGroupFromCollection(collectionIdPtr, collectionIdLength, groupsPtr, groupNamePtr, groupNameLength, groupsLength, &dependencies, caller)
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
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	// Read the result of the function.
	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	value, err := deps.DataManager.GetRecord(collId, recordName, groups)
	var ret int32
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			ret = wasmComponent.WriteError(Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			ret = wasmComponent.WriteError(NotFound, err)
		} else {
			ret = wasmComponent.WriteError(RedisError, err)
		}
	} else {
		ret = wasmComponent.WriteResult(value)
	}

	return ret
}

func hostWasmtimeMultipleGet(collIdPtr int32, collIdLength int32, recordNamesPtr int32, recordNamesLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	var recordNames []string
	err = wasmComponent.GetWasmObject(recordNamesPtr, recordNamesLength, &recordNames)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	values, err := deps.DataManager.GetRecords(collId, groups, recordNames...)
	var ret int32
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			ret = wasmComponent.WriteError(Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			ret = wasmComponent.WriteError(NotFound, err)
		} else {
			ret = wasmComponent.WriteError(RedisError, err)
		}
	} else {
		ret = wasmComponent.WriteArrayResult(values)
	}

	return ret
}

func hostWasmtimePut(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, valuePtr int32, valueLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	value, err := wasmComponent.GetWasmString(valuePtr, valueLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	err = deps.DataManager.SetRecord(collId, recordName, value, groups)
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			return wasmComponent.WriteError(Unauthorized, err)
		} else {
			return wasmComponent.WriteError(RedisError, err)
		}
	}

	return int32(wasm.Success)
}

func hostWasmtimeDelete(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	err = deps.DataManager.DeleteRecords(collId, groups, recordName)
	var ret int32
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			ret = wasmComponent.WriteError(Unauthorized, err)
		} else {
			ret = wasmComponent.WriteError(RedisError, err)
		}
	} else {
		ret = int32(wasm.Success)
	}

	return ret
}

func hostWasmtimeCreateCollection(metadataPointer int32, metadataLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var metadata utils.Metadata
	err := wasmComponent.GetWasmObject(metadataPointer, metadataLength, &metadata)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	groups := make([]any, len(metadata.Groups))
	for i, v := range metadata.Groups {
		groups[i] = v
	}

	collectionId := uuid.NewString()

	err = deps.DataManager.SetCollectionAndPropagate(collectionId, make(map[string]string), metadata.Groups, metadata.Consistency == utils.SequentialConsistent)
	if err != nil {
		return wasmComponent.WriteError(RedisError, err)
	}

	go deps.MetadataManager.CreateMetadata(collectionId, metadata, metadataManager.FROM_LOCAL)

	return wasmComponent.WriteResult(collectionId)
}

func hostWasmtimeDeleteCollection(collectionIdPtr int32, collectionIdLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	_, _ = deps, collectionId
	//TODO: err = deps.DataManager.DeleteCollection(collectionId)
	//TODO: Forward the deletion process to other nodes with HTTP

	return int32(wasm.Success)
}

func hostWasmtimeAddGroupToCollection(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	//TODO: chiamare DataManager per aggiornare il record speciale groups
	// e inoltrare l'aggiunta' del gruppo al record speciale agli altri nodi

	go deps.MetadataManager.AddGroupToCollection(collectionId, metadataManager.FROM_LOCAL, groupName)
	var ret int32
	if err != nil {
		ret = wasmComponent.WriteError(RedisError, err)
	} else {
		ret = int32(wasm.Success)
	}

	return ret
}

func hostWasmtimeRemoveGroupFromCollection(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	//TODO: chiamare DataManager per aggiornare il record speciale groups
	// e inoltrare la rimozione del gruppo dal record speciale agli altri nodi
	go deps.MetadataManager.RemoveGroupFromCollection(collectionId, metadataManager.FROM_LOCAL, groupName)

	var ret int32
	if err != nil {
		ret = wasmComponent.WriteError(RedisError, err)
	} else {
		ret = int32(wasm.Success)
	}

	return ret
}

func hostWasmtimeRemoveUserFromGroup(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusOK {
		ret = int32(wasm.Success)
	} else {
		ret = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}

func hostWasmtimeAddUserToGroup(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusOK {
		ret = int32(wasm.Success)
	} else {
		ret = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}

func hostWasmtimeCreateGroup(groupNamePtr int32, groupNameLength int32, partecipantIDsPtr int32, partecipantIDsLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	participantIDsJsonStr, err := wasmComponent.GetWasmString(partecipantIDsPtr, partecipantIDsLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName, bytes.NewBufferString(participantIDsJsonStr))
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusCreated {
		ret = int32(wasm.Success)
	} else {
		ret = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}

func hostWasmtimeDeleteGroup(groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies, caller *wasmtime.Caller) int32 {
	wasmComponent := wasm_utils.WasmtimeAdapter{
		Store:      caller,
		Memory:     caller.GetExport("memory").Memory(),
		MallocFunc: caller.GetExport("malloc").Func(),
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		return wasmComponent.WriteError(wasm.WasmRuntimeError, err)
	}

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName, nil)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		return wasmComponent.WriteError(HttpError, err)
	}
	_ = res.Body.Close()

	var ret int32
	if res.StatusCode == http.StatusOK {
		ret = int32(wasm.Success)
	} else {
		ret = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return ret
}
