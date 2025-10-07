package scheduler

import (
	"bytes"
	"context"
	shared "dataManager/shared"
	"errors"
	"fmt"
	"github.com/ermes-thesis/ermes-common/utils"
	"metadataManager"
	"net/http"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
	wasm_utils "github.com/ermes-thesis/ermes-common/wasm-pool/executor/utils"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/tetratelabs/wazero/api"
)

func RegisterWazeroHostFns(pool *wasm.WasmExecutorPool, dependencies HostFnsDependencies) error {
	fns := map[string]any{
		"hostGet": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength int32) int32 {
			return hostWazeroGet(ctx, m, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostMultipleGet": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength int32) int32 {
			return hostWazeroMultipleGet(ctx, m, collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostCreateCollection": func(ctx context.Context, m api.Module, metadataPointer, metadataLength int32) int32 {
			return hostWazeroCreateCollection(ctx, m, metadataPointer, metadataLength, &dependencies)
		},
		"hostPut": func(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamePtr, recordNameLength, valuePtr, valueLength, groupsPtr, groupsLength int32) int32 {
			return hostWazeroPut(ctx, m, collIdPtr, collIdLength, recordNamePtr, recordNameLength, valuePtr, valueLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostDelete": func(ctx context.Context, m api.Module, collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32) int32 {
			return hostWazeroDelete(ctx, m, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostDeleteCollection": func(ctx context.Context, m api.Module, collectionIdPtr int32, collectionIdLength int32, groupsPtr int32, groupsLength int32) int32 {
			return hostWazeroDeleteCollection(ctx, m, collectionIdPtr, collectionIdLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostAddGroupToCollection": func(ctx context.Context, m api.Module, collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32) int32 {
			return hostWazeroAddGroupToCollection(ctx, m, collectionIdPtr, collectionIdLength, groupNamePtr, groupNameLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostRemoveGroupFromCollection": func(ctx context.Context, m api.Module, collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32) int32 {
			return hostWazeroRemoveGroupFromCollection(ctx, m, collectionIdPtr, collectionIdLength, groupNamePtr, groupNameLength, groupsPtr, groupsLength, &dependencies)
		},
		"hostAddUserToGroup": func(ctx context.Context, m api.Module, userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32) int32 {
			return hostWazeroAddUserToGroup(ctx, m, userIdPtr, userIdLength, groupNamePtr, groupNameLength, &dependencies)
		},
		"hostRemoveUserFromGroup": func(ctx context.Context, m api.Module, userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32) int32 {
			return hostWazeroRemoveUserFromGroup(ctx, m, userIdPtr, userIdLength, groupNamePtr, groupNameLength, &dependencies)
		},
		"hostCreateGroup": func(ctx context.Context, m api.Module, groupNamePtr int32, groupNameLength int32, partecipantIDsPtr int32, partecipantIDsLength int32) int32 {
			return hostWazeroCreateGroup(ctx, m, groupNamePtr, groupNameLength, partecipantIDsPtr, partecipantIDsLength, &dependencies)
		},
		"hostDeleteGroup": func(ctx context.Context, m api.Module, groupNamePtr int32, groupNameLength int32) int32 {
			return hostWazeroDeleteGroup(ctx, m, groupNamePtr, groupNameLength, &dependencies)
		},
	}

	err := pool.RegisterHostFns(fns)
	if err != nil {
		return fmt.Errorf("failed to register module: %v", err)
	}
	return nil
}

func hostWazeroGet(ctx context.Context, m api.Module, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	value, err := deps.DataManager.GetRecord(collId, recordName, groups)
	var ret int32
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
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

func hostWazeroMultipleGet(ctx context.Context, m api.Module, collIdPtr int32, collIdLength int32, recordNamesPtr int32, recordNamesLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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
		if errors.Is(err, &shared.UnauthorizedError{}) {
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

func hostWazeroPut(ctx context.Context, m api.Module, collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, valuePtr int32, valueLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	err = deps.DataManager.SetRecord(collId, recordName, value, groups)
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			return wasmComponent.WriteError(model.Unauthorized, err)
		} else {
			return wasmComponent.WriteError(model.RedisError, err)
		}
	}

	return int32(model.Success)
}

func hostWazeroDelete(ctx context.Context, m api.Module, collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	err = deps.DataManager.DeleteRecords(collId, groups, recordName)
	var ret int32
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			ret = wasmComponent.WriteError(model.Unauthorized, err)
		} else {
			ret = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		ret = int32(model.Success)
	}

	return ret
}

func hostWazeroCreateCollection(ctx context.Context, m api.Module, metadataPointer int32, metadataLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	err = deps.DataManager.SetCollectionAndPropagate(collectionId, make(map[string]string), metadata.Groups, metadata.Consistency == model.SequentialConsistent)
	if err != nil {
		return wasmComponent.WriteError(model.RedisError, err)
	}

	go deps.MetadataManager.CreateMetadata(collectionId, metadata, metadataManager.FROM_LOCAL)

	return wasmComponent.WriteResult(collectionId)
}

func hostWazeroDeleteCollection(ctx context.Context, m api.Module, collectionIdPtr int32, collectionIdLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	_, _ = deps, collectionId
	//TODO: err = deps.DataManager.DeleteCollection(collectionId)
	//TODO: Forward the deletion process to other nodes with HTTP

	return int32(model.Success)
}

func hostWazeroAddGroupToCollection(ctx context.Context, m api.Module, collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	//TODO: chiamare DataManager per aggiornare il record speciale groups
	// e inoltrare l'aggiunta' del gruppo al record speciale agli altri nodi

	go deps.MetadataManager.AddGroupToCollection(collectionId, metadataManager.FROM_LOCAL, groupName)
	var ret int32
	if err != nil {
		ret = wasmComponent.WriteError(model.RedisError, err)
	} else {
		ret = int32(model.Success)
	}

	return ret
}

func hostWazeroRemoveGroupFromCollection(ctx context.Context, m api.Module, collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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

	//TODO: chiamare DataManager per aggiornare il record speciale groups
	// e inoltrare la rimozione del gruppo dal record speciale agli altri nodi
	go deps.MetadataManager.RemoveGroupFromCollection(collectionId, metadataManager.FROM_LOCAL, groupName)

	var ret int32
	if err != nil {
		ret = wasmComponent.WriteError(model.RedisError, err)
	} else {
		ret = int32(model.Success)
	}

	return ret
}

func hostWazeroRemoveUserFromGroup(ctx context.Context, m api.Module, userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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
	res, err := utils.CustomClient.Do(req)
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

func hostWazeroAddUserToGroup(ctx context.Context, m api.Module, userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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
	res, err := utils.CustomClient.Do(req)
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

func hostWazeroCreateGroup(ctx context.Context, m api.Module, groupNamePtr int32, groupNameLength int32, partecipantIDsPtr int32, partecipantIDsLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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
	res, err := utils.CustomClient.Do(req)
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

func hostWazeroDeleteGroup(ctx context.Context, m api.Module, groupNamePtr int32, groupNameLength int32, deps *HostFnsDependencies) int32 {
	_ = ctx
	wasmComponent := wasm_utils.WazeroAdapter{
		Memory:     m.Memory(),
		MallocFunc: m.ExportedFunction("malloc"),
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
	res, err := utils.CustomClient.Do(req)
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
