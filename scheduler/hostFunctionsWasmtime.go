package scheduler

import (
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
		"hostPut": func(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNamesLength int32, valuePtr int32, valueLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostDelete": func(collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostDeleteCollection": func(collectionId int32, collectionIdLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostAddGroupToCollection": func(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostRemoveGroupFromCollection": func(collectionIdPtr int32, collectionIdLength int32, groupNamePtr int32, groupNameLength int32, groupsPtr int32, groupsLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostAddUserToGroup": func(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostRemoveUserFromGroup": func(userIdPtr int32, userIdLength int32, groupNamePtr int32, groupNameLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostCreateGroup": func(groupNamePtr int32, groupNameLength int32, partecipantIDsPtr int32, partecipantIDsLength int32, caller *wasmtime.Caller) int32 {
			return 0
		},
		"hostDeleteGroup": func(groupNamePtr int32, groupNameLength int32, caller *wasmtime.Caller) int32 {
			return 0
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
