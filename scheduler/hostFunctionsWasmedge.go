package scheduler

import (
	datamanager "dataManager"
	shared "dataManager/shared"
	"errors"
	"fmt"
	"metadataManager"
	"server/info"

	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/second-state/WasmEdge-go/wasmedge"
)

type HostFnsDependencies struct {
	DataManager     datamanager.DataManager
	MetadataManager metadataManager.MetadataManager
	ServerInfo      *info.ServerInfo
}

func RegisterHostFns(pool *wasm.WasmExecutorPool, dependencies HostFnsDependencies) error {
	fnOneParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnThreeParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	// Create a function context with the function type and host function body.
	// The third parameter is the pointer to the additional data.
	// Developers should guarantee the life cycle of the data, and it can be
	// `nil` if the external data is not needed.
	// The last parameter can be 0 if developers do not need the cost measuring.
	fns := map[string]any{
		"hostGet":              wasmedge.NewFunction(fnThreeParamOneReturnType, hostGet, &dependencies, 0),
		"hostMultipleGet":      wasmedge.NewFunction(fnThreeParamOneReturnType, hostMultipleGet, &dependencies, 0),
		"hostCreateCollection": wasmedge.NewFunction(fnOneParamOneReturnType, hostCreateCollection, &dependencies, 0),
	}
	// If the function object is not added into a module instance object, it should be released.
	fnOneParamOneReturnType.Release()
	fnThreeParamOneReturnType.Release()

	err := pool.RegisterHostFns(fns)
	if err != nil {
		return fmt.Errorf("failed to register module: %v", err)
	}
	return nil
}

func hostGet(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collIdPtr := params[0].(int32)
	collIdLength := params[1].(int32)
	recordNamePtr := params[2].(int32)
	recordNameLength := params[3].(int32)
	groupsPtr := params[4].(int32)
	groupsLength := params[5].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := adapter.WasmedgeAdapter{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	// Read the result of the function.
	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	value, err := deps.DataManager.GetRecord(collId, groups, recordName)
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			returns[0] = wasmComponent.WriteError(model.Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			returns[0] = wasmComponent.WriteError(model.NotFound, err)
		} else {
			returns[0] = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		returns[0] = wasmComponent.WriteResult(value)
	}

	return returns, wasmedge.Result_Success
}

func hostMultipleGet(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collIdPtr := params[0].(int32)
	collIdLength := params[1].(int32)
	recordNamesPtr := params[2].(int32)
	recordNamesLength := params[3].(int32)
	groupsPtr := params[4].(int32)
	groupsLength := params[5].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := adapter.WasmedgeAdapter{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	var recordNames []string
	err = wasmComponent.GetWasmObject(recordNamesPtr, recordNamesLength, &recordNames)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	values, err := deps.DataManager.GetRecords(collId, groups, recordNames...)
	if err != nil {
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			returns[0] = wasmComponent.WriteError(model.Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			returns[0] = wasmComponent.WriteError(model.NotFound, err)
		} else {
			returns[0] = wasmComponent.WriteError(model.RedisError, err)
		}
	} else {
		returns[0] = wasmComponent.WriteArrayResult(values)
	}

	return returns, wasmedge.Result_Success
}

func hostCreateCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	metadataPointer := params[0].(int32)
	metadataLength := params[1].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := adapter.WasmedgeAdapter{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var metadata model.Metadata
	err := wasmComponent.GetWasmObject(metadataPointer, metadataLength, &metadata)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	groups := make([]any, len(metadata.Groups))
	for i, v := range metadata.Groups {
		groups[i] = v
	}

	collectionId := uuid.NewString()

	//TODO: this method should contact the cloud node and verify if there is already a collection with the same name and owner.
	// If not the existence of the collection must be registered while returning the responses.
	err = deps.DataManager.SetCollectionAndPropagate(collectionId, make(map[string]string), metadata.Groups, 0, metadata.Consistency == model.SequentialConsistent)
	if err != nil {
		returns[0] = wasmComponent.WriteError(model.RedisError, err)
		return returns, wasmedge.Result_Success
	}
	returns[0] = wasmComponent.WriteResult(collectionId)

	go deps.MetadataManager.CreateMetadata(collectionId, metadata, metadataManager.FROM_LOCAL)

	return returns, wasmedge.Result_Success
}
