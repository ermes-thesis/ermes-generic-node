package metadataManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/propagation"
	"net/http"
	"server/helper"

	"github.com/ermes-thesis/ermes-common/model"
	routing "github.com/qiangxue/fasthttp-routing"
)

func (m *MetadataManagerImpl) handleRouteCreateMetadata(ctx *routing.Context) error {

	// TODO: we need to check the equivalent of r.PathValue() in fasthttp. It should be .Param()

	collectionId := ctx.Param("collection_id")
	reqMetadataDomain := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-METADATA-DOMAIN"))

	m.log.Eval("Received metadata creation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "node_geolocation", m.serverInfo.MetadataDomain)
	m.log.Debug("Received metadata creation request", "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain)

	var meta model.Metadata
	if err := json.Unmarshal(ctx.PostBody(), &meta); err != nil {
		m.log.Error("Failed to decode metadata payload", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain, "error", err)
		ctx.SetStatusCode(http.StatusInternalServerError)
		return errors.New("failed to decode metadata payload")
	}

	var reqOrigin propagation.PropagationOrigin
	if m.serverInfo.Father != nil && reqMetadataDomain == m.serverInfo.Father.MetadataDomain {
		reqOrigin = propagation.FROM_FATHER
	} else {
		reqOrigin = propagation.FROM_CHILD
	}

	m.log.Debug("Request body decoded successfully", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", meta), "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)

	err := m.CreateMetadata(collectionId, meta, reqOrigin)
	if err != nil {
		m.log.Error("Failed to create metadata", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", meta), "request_origin", reqOrigin, "error", err)
		ctx.Error(fmt.Sprintf("Internal Server Error: failed to create metadata"), http.StatusInternalServerError)
		return errors.New(fmt.Sprintf("Internal Server Error: failed to create metadata"))
	}

	m.log.Info("Metadata created successfully", "collection_id", collectionId)
	m.log.Debug("Metadata created successfully", "metadata", fmt.Sprintf("%+v", meta), "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)
	ctx.SetStatusCode(http.StatusCreated)
	return nil
}
