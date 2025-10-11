package scheduler

import (
	shared "dataManager/shared"
	"errors"
	"metadataManager"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func hostGet(wasmComponent adapter.WasmRuntimeAdapter, collIdPtr int32, collIdLength int32, recordNamePtr int32, recordNameLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
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

func hostMultipleGet(wasmComponent adapter.WasmRuntimeAdapter, collIdPtr int32, collIdLength int32, recordNamesPtr int32, recordNamesLength int32, groupsPtr int32, groupsLength int32, deps *HostFnsDependencies) int32 {
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

func hostCreateCollection(wasmComponent adapter.WasmRuntimeAdapter, metadataPointer int32, metadataLength int32, deps *HostFnsDependencies) int32 {
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
