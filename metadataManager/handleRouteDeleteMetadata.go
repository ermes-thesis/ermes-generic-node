package metadataManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/propagation"
	"net/http"
	"server/helper"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (m *MetadataManagerImpl) handleRouteDeleteMetadata(ctx *routing.Context) error {

	collectionId := ctx.Param("collection_id")
	reqMetadataDomain := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-METADATA-DOMAIN"))
	isCollRecreated := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-COLLECTION-RECREATED")) == "true"

	m.log.Eval("Received metadata deletion request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "node_geolocation", m.serverInfo.MetadataDomain)
	m.log.Debug("Received metadata deletion request", "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain)

	var geolocation string
	if err := json.Unmarshal(ctx.PostBody(), &geolocation); err != nil {
		m.log.Error("Failed to decode request payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain, "error", err)
		ctx.SetStatusCode(http.StatusInternalServerError)
		return errors.New("failed to decode request payload")
	}

	var reqOrigin propagation.PropagationOrigin
	if m.serverInfo.Father != nil && reqMetadataDomain == m.serverInfo.Father.MetadataDomain {
		reqOrigin = propagation.FROM_FATHER
	} else {
		reqOrigin = propagation.FROM_CHILD
	}

	m.log.Debug("Request body decoded successfully", "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)

	err := m.DeleteMetadata(collectionId, geolocation, reqOrigin, isCollRecreated)
	if err != nil {
		m.log.Error("Failed to delete metadata", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: failed to create metadata"), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: failed to create metadata"))
	}

	m.log.Info("Metadata deleted successfully", "collection_id", collectionId)
	m.log.Debug("Metadata deleted successfully", "metadata_geolocation", geolocation, "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)
	ctx.SetStatusCode(http.StatusOK)
	return nil
}
