package scheduler

import (
	datamanager "dataManager"
	shared "dataManager/shared"
	"db"
	"errors"
	"fmt"
	"metadataManager"
	"metadataManager/propagation"
	"server"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/wasm-pool/adapter"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type HostFunctions struct {
	dataManager     datamanager.DataManager
	metadataManager metadataManager.MetadataManager
	serverInfo      *server.ServerInfo
	logger          logger.Logger
}

func NewHostFunctions(dataManager datamanager.DataManager, metadataManager metadataManager.MetadataManager, serverInfo *server.ServerInfo, logger logger.Logger) *HostFunctions {
	return &HostFunctions{
		dataManager:     dataManager,
		metadataManager: metadataManager,
		serverInfo:      serverInfo,
		logger:          logger,
	}
}

func (h *HostFunctions) hostGet(wasmComponent adapter.WasmRuntimeAdapter, collIdPtr, collIdLength, recordNamePtr, recordNameLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32) int32 {
	h.logger.Debug("HostGet called")

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		h.logger.Error("Error getting collection ID", "coll_id_ptr", collIdPtr, "coll_id_length", collIdLength, "record_name_ptr", recordNamePtr, "record_name_length", recordNameLength, "groups_ptr", groupsPtr, "groups_length", groupsLength, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	recordName, err := wasmComponent.GetWasmString(recordNamePtr, recordNameLength)
	if err != nil {
		h.logger.Error("Error getting record name", "coll_id", collId, "record_name_ptr", recordNamePtr, "record_name_length", recordNameLength, "groups_ptr", groupsPtr, "groups_length", groupsLength, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		h.logger.Error("Error getting groups", "coll_id", collId, "record_name", recordName, "groups_ptr", groupsPtr, "groups_length", groupsLength, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		h.logger.Error("Error getting user ID", "coll_id", collId, "record_name", recordName, "groups", groups, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	h.logger.Debugf("HostGet called with: collId=%s, recordName=%s, userId=%s, groups=%v", collId, recordName, userId, groups)

	value, err := h.dataManager.GetRecord(collId, groups, userId, recordName)
	if err != nil {
		h.logger.Debug("HostGet error retrieving record", "collection_id", collId, "record_name", recordName, "groups", groups, "user_id", userId, "error", err)
		var unAuthErr *shared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			return wasmComponent.WriteError(model.Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			return wasmComponent.WriteError(model.NotFound, err)
		}
		return wasmComponent.WriteError(model.RedisError, err)
	}

	h.logger.Debug("HostGet record retrieved successfully", "collection_id", collId, "record_name", recordName, "groups", groups, "user_id", userId, "value", value)
	return wasmComponent.WriteResult(value)
}

func (h *HostFunctions) hostMultipleGet(wasmComponent adapter.WasmRuntimeAdapter, collIdPtr, collIdLength, recordNamesPtr, recordNamesLength, groupsPtr, groupsLength, userIdPtr, userIdLength int32) int32 {
	h.logger.Debug("HostMultipleGet called")

	collId, err := wasmComponent.GetWasmString(collIdPtr, collIdLength)
	if err != nil {
		h.logger.Error("Error getting collection ID", "coll_id_ptr", collIdPtr, "coll_id_length", collIdLength, "record_names_ptr", recordNamesPtr, "record_names_length", recordNamesLength, "groups_ptr", groupsPtr, "groups_length", groupsLength, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var recordNames []string
	err = wasmComponent.GetWasmObject(recordNamesPtr, recordNamesLength, &recordNames)
	if err != nil {
		h.logger.Error("Error getting records names", "coll_id", collId, "record_names_ptr", recordNamesPtr, "record_names_length", recordNamesLength, "groups_ptr", groupsPtr, "groups_length", groupsLength, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	var groups []string
	err = wasmComponent.GetWasmObject(groupsPtr, groupsLength, &groups)
	if err != nil {
		h.logger.Error("Error getting groups", "coll_id", collId, "record_names", recordNames, "groups_ptr", groupsPtr, "groups_length", groupsLength, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	userId, err := wasmComponent.GetWasmString(userIdPtr, userIdLength)
	if err != nil {
		h.logger.Error("Error getting user ID", "coll_id", collId, "record_names", recordNames, "groups", groups, "user_id_ptr", userIdPtr, "user_id_length", userIdLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	h.logger.Debugf("HostMultipleGet called with: collId=%s, recordNames=%s, userId=%s, groups=%v", collId, recordNames, userId, groups)

	values, err := h.dataManager.GetRecords(collId, groups, userId, recordNames...)
	if err != nil {
		h.logger.Debug("HostMultipleGet error retrieving records", "collection_id", collId, "record_names", recordNames, "groups", groups, "user_id", userId, "error", err)
		if errors.Is(err, &shared.UnauthorizedError{}) {
			return wasmComponent.WriteError(model.Unauthorized, err)
		} else if errors.Is(err, redis.Nil) {
			return wasmComponent.WriteError(model.NotFound, err)
		}
		return wasmComponent.WriteError(model.RedisError, err)
	}

	h.logger.Debug("HostMultipleGet record retrieved successfully", "collection_id", collId, "record_names", recordNames, "groups", groups, "user_id", userId, "values", values)
	return wasmComponent.WriteArrayResult(values)
}

func (h *HostFunctions) hostCreateCollection(wasmComponent adapter.WasmRuntimeAdapter, metadataPointer, metadataLength, skipUniquenessCheck int32) int32 {
	h.logger.Debug("HostCreateCollection called")

	var metadata model.Metadata
	err := wasmComponent.GetWasmObject(metadataPointer, metadataLength, &metadata)
	if err != nil {
		h.logger.Error("Error getting collection's metadata", "metadata_ptr", metadataPointer, "metadata_length", metadataLength, "error", err)
		return wasmComponent.WriteError(model.WasmRuntimeError, err)
	}

	if _, ok := h.serverInfo.ExistingDomains[metadata.Geolocation]; !ok {
		errMsg := fmt.Sprintf("Domain %s not recognized in the system", metadata.Geolocation)
		h.logger.Error(errMsg, "metadata", fmt.Sprintf("%+v", metadata))
		return wasmComponent.WriteError(model.NotFound, errors.New(errMsg))
	}

	skipUniquenessCheckBool := skipUniquenessCheck != 0

	groups := make([]any, len(metadata.Groups))
	for i, v := range metadata.Groups {
		groups[i] = v
	}

	h.logger.Debugf("HostCreateCollection called with: metadata=%+v", metadata)

	collectionId := uuid.NewString()

	h.logger.Debugf("HostCreateCollection generated collectionId=%s", collectionId)

	err = h.dataManager.SetCollectionAndPropagate(collectionId, metadata.Name, make(map[string]db.RecordValue), metadata.Groups, metadata.Owner, metadata.Consistency == model.SequentialConsistent, skipUniquenessCheckBool)
	if err != nil {
		if h.logger.Enabled(logger.DebugLevel) {
			h.logger.Debug("HostCreateCollection error creating collection", "metadata", fmt.Sprintf("%+v", metadata), "error", err)
		}
		return wasmComponent.WriteError(model.RedisError, err)
	}

	h.logger.Debugf("HostCreateCollection created collection successfully, propagating metadata asynchronously")
	go func() {
		err = h.metadataManager.CreateMetadata(collectionId, metadata, propagation.FROM_LOCAL)
		if err != nil {
			if h.logger.Enabled(logger.DebugLevel) {
				h.logger.Debug("HostCreateCollection error creating metadata", "metadata", fmt.Sprintf("%+v", metadata), "collection_id", collectionId, "error", err)
			}
		}
	}()

	if h.logger.Enabled(logger.DebugLevel) {
		h.logger.Debug("HostCreateCollection completed successfully", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata))
	}
	return wasmComponent.WriteResult(collectionId)
}
