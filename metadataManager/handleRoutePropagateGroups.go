package metadataManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/propagation"
	"net/http"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (m *MetadataManagerImpl) handleRoutePropagateGroups(ctx *routing.Context) error {

	collectionId := ctx.Param("collection_id")

	m.log.Eval("Received group update propagation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI())
	m.log.Debug("Received group update propagation request", "collection_id", collectionId, "node_geolocation", m.serverInfo.MetadataDomain)

	var message propagation.UpdateGroupResponse
	if err := json.Unmarshal(ctx.PostBody(), &message); err != nil {
		m.log.Error("Failed to decode group propagation payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "node_geolocation", m.serverInfo.MetadataDomain, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: invalid request body: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: invalid request body: %v", err))
	}

	m.log.Debug("Decoded group propagation payload", "collection_id", collectionId, "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence)

	err := m.SetCollectionGroups(collectionId, message)
	if err != nil {
		m.log.Error("Failed to propagate group updates", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: invalid request body: %v", err), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: invalid request body: %v", err))
	}

	m.log.Info("Group propagation completed successfully for the current node", "collection_id", collectionId, "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence)
	ctx.SetStatusCode(http.StatusOK)
	return nil
}
