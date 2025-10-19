package scheduler

import (
	datamanager "dataManager"
	shared "dataManager/shared"
	"errors"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/wasm-pool/executor/adapter"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"metadataManager"
	"metadataManager/propagation"
	"server"
)

type HostFunctions struct {
	dataManager     datamanager.DataManager
	metadataManager metadataManager.MetadataManager
	serverInfo      *server.ServerInfo
	logger          *logger.Logger
}

func NewHostFunctions(dataManager datamanager.DataManager, metadataManager metadataManager.MetadataManager, serverInfo *server.ServerInfo, logger *logger.Logger) *HostFunctions {
	return &HostFunctions{
		dataManager:     dataManager,
		metadataManager: metadataManager,
		serverInfo:      serverInfo,
		logger:          logger,
	}
}

func (h *HostFunctions) hostGet(wasmComponent adapter.WasmRuntimeAdapter, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32) int32 {
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

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	value, err := h.dataManager.GetRecord(collId, groups, userId, recordName)
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

func (h *HostFunctions) hostMultipleGet(wasmComponent adapter.WasmRuntimeAdapter, collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32) int32 {
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

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	values, err := h.dataManager.GetRecords(collId, groups, userId, recordNames...)
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

func (h *HostFunctions) hostCreateCollection(wasmComponent adapter.WasmRuntimeAdapter, metadataPointer int32, metadataLength int32) int32 {
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

	err = h.dataManager.SetCollectionAndPropagate(collectionId, metadata.Name, make(map[string]string), metadata.Groups, metadata.Owner, metadata.Consistency == model.SequentialConsistent)
	if err != nil {
		return wasmComponent.WriteError(model.RedisError, err)
	}

	go h.metadataManager.CreateMetadata(collectionId, metadata, propagation.FROM_LOCAL)

	return wasmComponent.WriteResult(collectionId)
}
