package scheduler

import (
	"context"
	shared "dataManager/shared"
	"errors"
	"fmt"
	"metadataManager"

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

	value, err := deps.DataManager.GetRecord(collId, groups, recordName)
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

	err = deps.DataManager.SetCollectionAndPropagate(collectionId, make(map[string]string), metadata.Groups, 0, metadata.Consistency == model.SequentialConsistent)
	if err != nil {
		return wasmComponent.WriteError(model.RedisError, err)
	}

	go deps.MetadataManager.CreateMetadata(collectionId, metadata, metadataManager.FROM_LOCAL)

	return wasmComponent.WriteResult(collectionId)
}
