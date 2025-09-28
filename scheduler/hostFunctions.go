package scheduler

import (
	"bytes"
	datamanager "dataManager"
	shared "dataManager/shared"
	"errors"
	"fmt"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"server/client"
	"server/info"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/second-state/WasmEdge-go/wasmedge"
)

type HostFnsDependencies struct {
	DataManager     datamanager.DataManager
	MetadataManager metadataManager.MetadataManager
	ServerInfo      *info.ServerInfo
}

const (
	Unauthorized wasm.ResponseCode = iota + 100
	NotFound
	RedisError
	GroupTokenProviderError
	HttpError
)

func RegisterHostFns(pool *wasm.WasmExecutorPool, dependencies HostFnsDependencies) error {
	// Create the module instance with the module name "extern".
	externMod := wasmedge.NewModule("extern")

	fnOneParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnTwoParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnThreeParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnFourParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	// Create a function context with the function type and host function body.
	// The third parameter is the pointer to the additional data.
	// Developers should guarantee the life cycle of the data, and it can be
	// `nil` if the external data is not needed.
	// The last parameter can be 0 if developers do not need the cost measuring.
	hostGetFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostGet, &dependencies, 0)
	hostMultipleGetFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostMultipleGet, &dependencies, 0)
	hostPutFun := wasmedge.NewFunction(fnFourParamOneReturnType, hostPut, &dependencies, 0)
	hostDelFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostDelete, &dependencies, 0)
	hostCreateCollectionFun := wasmedge.NewFunction(fnOneParamOneReturnType, hostCreateCollection, &dependencies, 0)
	hostDeleteCollectionFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostDeleteCollection, &dependencies, 0)
	hostAddGroupToCollectionFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostAddGroupToCollection, &dependencies, 0)
	hostRemoveGroupFromCollectionFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostRemoveGroupFromCollection, &dependencies, 0)
	hostAddUserToGroupFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostAddUserToGroup, &dependencies, 0)
	hostRemoveUserFromGroupFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostRemoveUserFromGroup, &dependencies, 0)
	hostCreateGroupFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostCreateGroup, &dependencies, 0)
	hostDeleteGroupFun := wasmedge.NewFunction(fnOneParamOneReturnType, hostDeleteGroup, &dependencies, 0)

	// If the function object is not added into a module instance object, it should be released.
	fnOneParamOneReturnType.Release()
	fnTwoParamOneReturnType.Release()
	fnThreeParamOneReturnType.Release()
	fnFourParamOneReturnType.Release()
	externMod.AddFunction("hostGet", hostGetFun)
	externMod.AddFunction("hostMultipleGet", hostMultipleGetFun)
	externMod.AddFunction("hostPut", hostPutFun)
	externMod.AddFunction("hostDelete", hostDelFun)
	externMod.AddFunction("hostCreateCollection", hostCreateCollectionFun)
	externMod.AddFunction("hostDeleteCollection", hostDeleteCollectionFun)
	externMod.AddFunction("hostAddGroupToCollection", hostAddGroupToCollectionFun)
	externMod.AddFunction("hostRemoveGroupFromCollection", hostRemoveGroupFromCollectionFun)
	externMod.AddFunction("hostAddUserToGroup", hostAddUserToGroupFun)
	externMod.AddFunction("hostRemoveUserFromGroup", hostRemoveUserFromGroupFun)
	externMod.AddFunction("hostCreateGroup", hostCreateGroupFun)
	externMod.AddFunction("hostDeleteGroup", hostDeleteGroupFun)

	err := pool.RegisterModule(externMod)
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

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	// Read the result of the function.
	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	value, err := deps.DataManager.GetRecord(collId, recordName, groups)

	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			returns[0] = wasmComponent.WriteError(Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			returns[0] = wasmComponent.WriteError(NotFound, err)
		} else {
			returns[0] = wasmComponent.WriteError(RedisError, err)
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

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	var recordNames []string
	err = wasmComponent.GetWasmObject(recordNamesPtr, recordNamesLength, &recordNames)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	values, err := deps.DataManager.GetRecords(collId, groups, recordNames...)
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			returns[0] = wasmComponent.WriteError(Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			returns[0] = wasmComponent.WriteError(NotFound, err)
		} else {
			returns[0] = wasmComponent.WriteError(RedisError, err)
		}
	} else {
		returns[0] = wasmComponent.WriteArrayResult(values)
	}

	return returns, wasmedge.Result_Success
}

func hostPut(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collIdPtr := params[0].(int32)
	collIdLength := params[1].(int32)
	recordNamePtr := params[2].(int32)
	recordNameLength := params[3].(int32)
	valuePointer := params[4].(int32)
	valueLength := params[5].(int32)
	groupsPtr := params[6].(int32)
	groupsLength := params[7].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	value, err := wasmComponent.GetWasmString(valuePointer, valueLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	err = deps.DataManager.SetRecord(collId, recordName, value, groups)
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			returns[0] = wasmComponent.WriteError(Unauthorized, err)
		} else {
			returns[0] = wasmComponent.WriteError(RedisError, err)
		}
	} else {
		returns[0] = int32(wasm.Success)
	}

	return returns, wasmedge.Result_Success
}

func hostDelete(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collIdPtr := params[0].(int32)
	collIdLength := params[1].(int32)
	recordNamePtr := params[2].(int32)
	recordNameLength := params[3].(int32)
	groupsPtr := params[4].(int32)
	groupsLength := params[5].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	_, _, _ = deps, collId, recordName
	//TODO: err = deps.DataManager.Delete
	if err != nil {
		if errors.Is(err, &shared.UnauthorizedError{}) {
			returns[0] = wasmComponent.WriteError(Unauthorized, err)
		} else {
			returns[0] = wasmComponent.WriteError(RedisError, err)
		}
	} else {
		returns[0] = int32(wasm.Success)
	}

	return returns, wasmedge.Result_Success
}

func hostCreateCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	metadataPointer := params[0].(int32)
	metadataLength := params[1].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var metadata utils.Metadata
	err := wasmComponent.GetWasmObject(metadataPointer, metadataLength, &metadata)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	groups := make([]any, len(metadata.Groups))
	for i, v := range metadata.Groups {
		groups[i] = v
	}

	collectionId := uuid.NewString()

	// TODO: Substitute with dataManager
	//err = deps.RedisClient.SAdd(context.Background(), key, groups...).Err()
	if err != nil {
		returns[0] = wasmComponent.WriteError(RedisError, err)
	} else {
		returns[0] = wasmComponent.WriteResult(collectionId)
	}

	go deps.MetadataManager.CreateMetadata(collectionId, metadata, metadataManager.FROM_LOCAL)

	return returns, wasmedge.Result_Success
}

func hostDeleteCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collectionIdPtr := params[0].(int32)
	collectionIdLength := params[1].(int32)
	groupsPtr := params[4].(int32)
	groupsLength := params[5].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	_, _ = deps, collectionId
	//TODO: err = deps.DataManager.DeleteCollection(collectionId)
	//TODO: Forward the deletion process to other nodes with HTTP

	returns[0] = int32(wasm.Success)
	return returns, wasmedge.Result_Success
}

func hostAddGroupToCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collectionIdPointer := params[0].(int32)
	collectionIdLength := params[1].(int32)
	groupNamePointer := params[2].(int32)
	groupNameLength := params[3].(int32)
	groupsPtr := params[4].(int32)
	groupsLength := params[5].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}
	returns := make([]any, 1)

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPointer, collectionIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePointer, groupNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	//TODO: chiamare DataManager per aggiornare il record speciale groups
	// e inoltrare l'aggiunta' del gruppo al record speciale agli altri nodi

	go deps.MetadataManager.AddGroupToCollection(collectionId, metadataManager.FROM_LOCAL, groupName)
	if err != nil {
		returns[0] = wasmComponent.WriteError(RedisError, err)
		return returns, wasmedge.Result_Success
	} else {
		returns[0] = int32(wasm.Success)
	}

	return returns, wasmedge.Result_Success
}

func hostRemoveGroupFromCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collectionIdPointer := params[0].(int32)
	collectionIdLength := params[1].(int32)
	groupNamePointer := params[2].(int32)
	groupNameLength := params[3].(int32)
	groupsPtr := params[4].(int32)
	groupsLength := params[5].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}
	returns := make([]any, 1)

	var groups []string
	err := wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPointer, collectionIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	groupName, err := wasmComponent.GetWasmString(groupNamePointer, groupNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	//TODO: chiamare DataManager per aggiornare il record speciale groups
	// e inoltrare la rimozione del gruppo dal record speciale agli altri nodi
	go deps.MetadataManager.RemoveGroupFromCollection(collectionId, metadataManager.FROM_LOCAL, groupName)

	if err != nil {
		returns[0] = wasmComponent.WriteError(RedisError, err)
	} else {
		returns[0] = int32(wasm.Success)
	}

	return returns, wasmedge.Result_Success
}

func hostRemoveUserFromGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	userIdPtr := params[0].(int32)
	userIdLength := params[1].(int32)
	groupNamePtr := params[2].(int32)
	groupNameLength := params[3].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusOK {
		returns[0] = int32(wasm.Success)
	} else {
		returns[0] = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}

func hostAddUserToGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	userIdPtr := params[0].(int32)
	userIdLength := params[1].(int32)
	groupNamePtr := params[2].(int32)
	groupNameLength := params[3].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusOK {
		returns[0] = int32(wasm.Success)
	} else {
		returns[0] = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}

func hostCreateGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	groupNamePtr := params[0].(int32)
	groupNameLength := params[1].(int32)
	participantIDsJsonPtr := params[2].(int32)
	participantIDsJsonLength := params[3].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	participantIDsJsonStr, err := wasmComponent.GetWasmString(participantIDsJsonPtr, participantIDsJsonLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName, bytes.NewBufferString(participantIDsJsonStr))
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusCreated {
		returns[0] = int32(wasm.Success)
	} else {
		returns[0] = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}

func hostDeleteGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	groupNamePtr := params[0].(int32)
	groupNameLength := params[1].(int32)

	deps := data.(*HostFnsDependencies)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WasmRuntimeError, err)
		return returns, wasmedge.Result_Success
	}

	groupTokenProviderAddr := fmt.Sprintf("http://%s", deps.ServerInfo.GroupTokenProviderHostName)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName, nil)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("X-ERMES-SECRET", deps.ServerInfo.Secret)
	res, err := client.CustomClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HttpError, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusOK {
		returns[0] = int32(wasm.Success)
	} else {
		returns[0] = wasmComponent.WriteError(GroupTokenProviderError, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}
