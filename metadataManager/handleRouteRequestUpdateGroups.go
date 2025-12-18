package metadataManager

import (
	dataManagerShared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/propagation"
	"net/http"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (m *MetadataManagerImpl) handleRouteRequestUpdateGroups(ctx *routing.Context) error {

	collectionId := ctx.Param("collection_id")

	m.log.Eval("Received group update new request for metadata", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())
	m.log.Debug("Received group update new request for metadata", "collection_id", collectionId)

	var message propagation.GroupMessage
	if err := json.Unmarshal(ctx.PostBody(), &message); err != nil {
		m.log.Error("Failed to decode request body", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "error", err)
		ctx.Error(fmt.Sprintf("Invalid request body: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Invalid request body: %v", err))
	}

	m.log.Debug("Decoded group update message", "collection_id", collectionId, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove)

	resp, err := m.UpdateGroupsInCollection(collectionId, message.UserGroups, message.UserId, message.GroupsToAdd, message.GroupsToRemove)
	if err != nil {
		var unAuthErr *dataManagerShared.UnauthorizedError
		if errors.As(err, &unAuthErr) {
			m.log.Warn("Unauthorized group update attempt", "collection_id", collectionId, "user_access_groups", message.UserGroups, "error", err)
			ctx.Error("Unauthorized", http.StatusUnauthorized)
			return errors.New("unauthorized group update attempt")
		}

		m.log.Error("Failed to update groups in collection", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: Failed to update groups in collection: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: Failed to update groups in collection: %v", err))
	}

	ctx.SetContentType("application/json")

	encoded, err := json.Marshal(resp)
	if err != nil {
		m.log.Error("Failed to convert response into json", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "new_group_set", resp.Groups, "new_group_sequence_num", resp.Sequence, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: Failed to convert response into json: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: Failed to convert response into json: %v", err))
	}

	_, err = ctx.Write(encoded)
	if err != nil {
		m.log.Error("Failed to encode response into body", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "new_group_set", resp.Groups, "new_group_sequence_num", resp.Sequence, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: error while encoding query results in response: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: Ferror while encoding query results in response: %v", err))
	}

	m.log.Info("Group update in collection's metadata handled successfully", "collection_id", collectionId)
	m.log.Debug("Group update in collection's metadata handled successfully", "new_groups", resp.Groups, "new_group_sequence_num", resp.Sequence)
	ctx.SetStatusCode(http.StatusOK)
	return nil
}
