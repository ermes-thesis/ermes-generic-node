package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"server/info"
	"slices"
	"strings"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/second-state/WasmEdge-go/wasmedge"
)

const (
	UNAUTHORIZED wasm.ResponseCode = iota + 100
	NOT_FOUND
	REDIS_ERROR
	GROUP_TOKEN_PROVIDER_ERROR
	HTTP_ERROR
)

func RegisterHostFns(pool *wasm.WasmExecutorPool, redisClient *redis.Client, serverInfo *info.ServerInfo) error {
	// Create the module instance with the module name "extern".
	externMod := wasmedge.NewModule("extern")

	fnTwoParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnThreeParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	fnOneParamOneReturnType := wasmedge.NewFunctionType(
		[]*wasmedge.ValType{wasmedge.NewValTypeI32(), wasmedge.NewValTypeI32()},
		[]*wasmedge.ValType{wasmedge.NewValTypeI32()},
	)
	// Create a function context with the function type and host function body.
	// The third parameter is the pointer to the additional data.
	// Developers should guarantee the life cycle of the data, and it can be
	// `nil` if the external data is not needed.
	// The last parameter can be 0 if developers do not need the cost measuring.
	hostGetFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostGet, redisClient, 0)
	hostMultipleGetFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostMultipleGet, redisClient, 0)
	hostPutFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostPut, redisClient, 0)
	hostDelFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostDelete, redisClient, 0)
	hostCreateCollectionFun := wasmedge.NewFunction(fnOneParamOneReturnType, hostCreateCollection, redisClient, 0)
	hostDeleteCollectionFun := wasmedge.NewFunction(fnOneParamOneReturnType, hostDeleteCollection, redisClient, 0)
	hostAddGroupToCollectionFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostAddGroupToCollection, redisClient, 0)
	hostRemoveGroupFromCollectionFun := wasmedge.NewFunction(fnThreeParamOneReturnType, hostRemoveGroupFromCollection, redisClient, 0)
	hostAddUserToGroupFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostAddUserToGroup, serverInfo, 0)
	hostRemoveUserFromGroupFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostRemoveUserFromGroup, serverInfo, 0)
	hostCreateGroupFun := wasmedge.NewFunction(fnTwoParamOneReturnType, hostCreateGroup, serverInfo, 0)
	hostDeleteGroupFun := wasmedge.NewFunction(fnOneParamOneReturnType, hostDeleteGroup, serverInfo, 0)

	// If the function object is not added into a module instance object, it should be released.
	fnTwoParamOneReturnType.Release()
	fnThreeParamOneReturnType.Release()
	fnOneParamOneReturnType.Release()
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
	keyPointer := params[0].(int32)
	keyLength := params[1].(int32)
	tokenPointer := params[2].(int32)
	tokenLength := params[3].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var groupToken []any
	err := wasmComponent.GetWasmObject(tokenPointer, tokenLength, &groupToken)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	// Read the result of the function.
	key, err := wasmComponent.GetWasmString(keyPointer, keyLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	collId := strings.Split(key, ":")[1]
	res, err := client.SMIsMember(context.Background(), "data:"+collId+":groups", groupToken...).Result()
	authorized := slices.Contains(res, true)

	if err != nil {
		returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
	} else if !authorized {
		returns[0] = wasmComponent.WriteError(UNAUTHORIZED, errors.New("current user is not authorized"))
	} else {
		//TODO: fare una chiamata remota se la collection non è presente in questo nodo
		value, err := client.Get(context.Background(), key).Result()
		if err != nil {
			returns[0] = wasmComponent.WriteError(NOT_FOUND, err)
		} else {
			returns[0] = wasmComponent.WriteResult(value)
		}
	}

	return returns, wasmedge.Result_Success
}

func hostMultipleGet(data interface{}, callframe *wasmedge.CallingFrame, params []interface{}) ([]interface{}, wasmedge.Result) {
	keysPointer := params[0].(int32)
	keysLength := params[1].(int32)
	tokenPointer := params[2].(int32)
	tokenLength := params[3].(int32)
	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]interface{}, 1)

	var keys []string
	err := wasmComponent.GetWasmObject(keysPointer, keysLength, &keys)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	var groupToken []string
	err = wasmComponent.GetWasmObject(tokenPointer, tokenLength, &groupToken)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	collId := strings.Split(keys[0], ":")[1]
	res, err := client.SMIsMember(context.Background(), "data:"+collId+":groups", groupToken).Result()
	authorized := slices.Contains(res, true)

	var resPtr any
	if err != nil {
		resPtr = wasmComponent.WriteError(REDIS_ERROR, err)
	} else if !authorized {
		resPtr = wasmComponent.WriteError(UNAUTHORIZED, errors.New("current user is not authorized"))
	} else {
		values, _ := client.MGet(context.Background(), keys...).Result()
		resPtr = wasmComponent.WriteArrayResult(values)
	}

	returns[0] = resPtr.(int32)
	return returns, wasmedge.Result_Success
}

func hostPut(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	keyPointer := params[0].(int32)
	keyLength := params[1].(int32)
	valuePointer := params[2].(int32)
	valueLength := params[3].(int32)
	tokenPointer := params[4].(int32)
	tokenLength := params[5].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var groupToken []any
	err := wasmComponent.GetWasmObject(tokenPointer, tokenLength, &groupToken)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	key, err := wasmComponent.GetWasmString(keyPointer, keyLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	collId := strings.Split(key, ":")[1]
	res, err := client.SMIsMember(context.Background(), "data:"+collId+":groups", groupToken...).Result()
	if err != nil {
		returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	authorized := slices.Contains(res, true)

	if !authorized {
		returns[0] = wasmComponent.WriteError(UNAUTHORIZED, errors.New("current user is not authorized"))
	} else {
		value, err := wasmComponent.GetWasmString(valuePointer, valueLength)
		if err != nil {
			returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
			return returns, wasmedge.Result_Success
		}
		err = client.Set(context.Background(), key, value, 0).Err()
		//TODO: inoltrare la put agli altri nodi
		if err != nil {
			returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
		} else {
			returns[0] = int32(wasm.SUCCESS)
		}
	}

	return returns, wasmedge.Result_Success
}

func hostDelete(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	keyPointer := params[0].(int32)
	keyLength := params[1].(int32)
	tokenPointer := params[2].(int32)
	tokenLength := params[3].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	var groupToken []any
	err := wasmComponent.GetWasmObject(tokenPointer, tokenLength, &groupToken)

	// Read the result of the function.
	key, err := wasmComponent.GetWasmString(keyPointer, keyLength)

	collId := strings.Split(key, ":")[1]
	res, err := client.SMIsMember(context.Background(), "data:"+collId+":groups", groupToken...).Result()
	authorized := slices.Contains(res, true)

	returns := make([]any, 1)
	if err != nil {
		returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
	} else if !authorized {
		returns[0] = wasmComponent.WriteError(UNAUTHORIZED, errors.New("current user is not authorized"))
	} else {
		err = client.Del(context.Background(), key).Err()

		//TODO: inoltrare la l'eliminazione agli altri nodi
		if err != nil {
			returns[0] = wasmComponent.WriteError(NOT_FOUND, err)
		} else {
			returns[0] = int32(wasm.SUCCESS)
		}
	}

	return returns, wasmedge.Result_Success
}

func hostAddGroupToCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collectionIdPointer := params[0].(int32)
	collectionIdLength := params[1].(int32)
	groupNamePointer := params[2].(int32)
	groupNameLength := params[3].(int32)
	tokenPointer := params[4].(int32)
	tokenLength := params[5].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}
	returns := make([]any, 1)

	var groupToken []any
	err := wasmComponent.GetWasmObject(tokenPointer, tokenLength, &groupToken)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPointer, collectionIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	key := "data:" + collectionId + ":groups"
	res, err := client.SMIsMember(context.Background(), key, groupToken...).Result()
	if err != nil {
		returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	authorized := slices.Contains(res, true)

	if !authorized {
		returns[0] = wasmComponent.WriteError(UNAUTHORIZED, errors.New("current user is not authorized"))
	} else {
		groupName, err := wasmComponent.GetWasmString(groupNamePointer, groupNameLength)
		if err != nil {
			returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
			return returns, wasmedge.Result_Success
		}

		err = client.SAdd(context.Background(), key, groupName).Err()

		//TODO: inoltrare l'aggiunta' del gruppo agli altri nodi
		//TODO: aggiungere il gruppo anche nei metadati

		if err != nil {
			returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
		} else {
			returns[0] = int32(wasm.SUCCESS)
		}
	}

	return returns, wasmedge.Result_Success
}

func hostRemoveGroupFromCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collectionIdPointer := params[0].(int32)
	collectionIdLength := params[1].(int32)
	groupNamePointer := params[2].(int32)
	groupNameLength := params[3].(int32)
	tokenPointer := params[4].(int32)
	tokenLength := params[5].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}
	returns := make([]any, 1)

	var groupToken []any
	err := wasmComponent.GetWasmObject(tokenPointer, tokenLength, &groupToken)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPointer, collectionIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	key := "data:" + collectionId + ":groups"
	res, err := client.SMIsMember(context.Background(), key, groupToken...).Result()
	if err != nil {
		returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	authorized := slices.Contains(res, true)

	if !authorized {
		returns[0] = wasmComponent.WriteError(UNAUTHORIZED, errors.New("current user is not authorized"))
	} else {
		groupName, err := wasmComponent.GetWasmString(groupNamePointer, groupNameLength)
		if err != nil {
			returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
			return returns, wasmedge.Result_Success
		}

		err = client.SRem(context.Background(), key, groupName).Err()

		//TODO: inoltrare la rimozione del gruppo agli altri nodi
		//TODO: rimuovere il gruppo anche dai metadati

		if err != nil {
			returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
		} else {
			returns[0] = int32(wasm.SUCCESS)
		}
	}

	return returns, wasmedge.Result_Success
}

func hostCreateCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	metadataPointer := params[0].(int32)
	metadataLength := params[1].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)

	var metadata map[string]any
	err := wasmComponent.GetWasmObject(metadataPointer, metadataLength, &metadata)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	groups := metadata["groups"].([]any)
	collectionId := uuid.NewString()
	key := "data:" + collectionId + ":groups"

	//TODO: mandare i metadati al cloud, che poi li manderà a sua volta al nodo con la geolocation specificata

	err = client.SAdd(context.Background(), key, groups...).Err()

	if err != nil {
		returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
	} else {
		returns[0] = wasmComponent.WriteResult(collectionId)
	}

	return returns, wasmedge.Result_Success
}

func hostDeleteCollection(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	collectionIdPtr := params[0].(int32)
	collectionIdLength := params[1].(int32)

	client := data.(*redis.Client)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	collectionId, err := wasmComponent.GetWasmString(collectionIdPtr, collectionIdLength)
	returns := make([]any, 1)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	var cursor uint64
	for {
		var keys []string
		keys, cursor, err = client.Scan(context.Background(), cursor, "data:"+collectionId+":*", 10).Result()
		if err != nil {
			returns[0] = wasmComponent.WriteError(REDIS_ERROR, err)
			return returns, wasmedge.Result_Success
		}

		client.Del(context.Background(), keys...)
		if cursor == 0 {
			break
		}
	}

	//TODO: Forward the deletion process to other nodes with HTTP

	returns[0] = int32(wasm.SUCCESS)
	return returns, wasmedge.Result_Success
}

func hostRemoveUserFromGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	userIdPtr := params[0].(int32)
	userIdLength := params[1].(int32)
	groupNamePtr := params[2].(int32)
	groupNameLength := params[3].(int32)

	serverInfo := data.(*info.ServerInfo)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", serverInfo.GroupTokenProvider)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("X-ERMES-SECRET", serverInfo.Secret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusOK {
		returns[0] = int32(wasm.SUCCESS)
	} else {
		returns[0] = wasmComponent.WriteError(GROUP_TOKEN_PROVIDER_ERROR, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}

func hostAddUserToGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	userIdPtr := params[0].(int32)
	userIdLength := params[1].(int32)
	groupNamePtr := params[2].(int32)
	groupNameLength := params[3].(int32)

	serverInfo := data.(*info.ServerInfo)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", serverInfo.GroupTokenProvider)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName+"/"+userId, nil)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("X-ERMES-SECRET", serverInfo.Secret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusOK {
		returns[0] = int32(wasm.SUCCESS)
	} else {
		returns[0] = wasmComponent.WriteError(GROUP_TOKEN_PROVIDER_ERROR, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}

func hostCreateGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	groupNamePtr := params[0].(int32)
	groupNameLength := params[1].(int32)
	participantIDsJsonPtr := params[2].(int32)
	participantIDsJsonLength := params[3].(int32)

	serverInfo := data.(*info.ServerInfo)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	participantIDsJsonStr, err := wasmComponent.GetWasmString(participantIDsJsonPtr, participantIDsJsonLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)

	groupTokenProviderAddr := fmt.Sprintf("http://%s", serverInfo.GroupTokenProvider)
	req, err := http.NewRequest(http.MethodPost, groupTokenProviderAddr+"/group/"+groupName, bytes.NewBufferString(participantIDsJsonStr))
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", serverInfo.Secret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusCreated {
		returns[0] = int32(wasm.SUCCESS)
	} else {
		returns[0] = wasmComponent.WriteError(GROUP_TOKEN_PROVIDER_ERROR, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}

func hostDeleteGroup(data any, callframe *wasmedge.CallingFrame, params []any) ([]any, wasmedge.Result) {
	groupNamePtr := params[0].(int32)
	groupNameLength := params[1].(int32)

	serverInfo := data.(*info.ServerInfo)

	wasmComponent := wasm.WasmComponents{
		Memory:     callframe.GetMemoryByIndex(0),
		Executor:   callframe.GetExecutor(),
		MallocFunc: callframe.GetModule().FindFunction("malloc"),
	}

	returns := make([]any, 1)
	groupName, err := wasmComponent.GetWasmString(groupNamePtr, groupNameLength)
	if err != nil {
		returns[0] = wasmComponent.WriteError(wasm.WASM_RUNTIME_ERROR, err)
		return returns, wasmedge.Result_Success
	}

	groupTokenProviderAddr := fmt.Sprintf("http://%s", serverInfo.GroupTokenProvider)
	req, err := http.NewRequest(http.MethodDelete, groupTokenProviderAddr+"/group/"+groupName, nil)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	req.Header.Set("X-ERMES-SECRET", serverInfo.Secret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		returns[0] = wasmComponent.WriteError(HTTP_ERROR, err)
		return returns, wasmedge.Result_Success
	}
	_ = res.Body.Close()

	if res.StatusCode == http.StatusOK {
		returns[0] = int32(wasm.SUCCESS)
	} else {
		returns[0] = wasmComponent.WriteError(GROUP_TOKEN_PROVIDER_ERROR, errors.New(res.Status))
	}
	return returns, wasmedge.Result_Success
}
