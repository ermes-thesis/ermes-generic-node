package propagation

import (
	"fmt"
	"net/http"
	"server"
	"strconv"
	"strings"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
)

type DeleteMetadataPropagator interface {
	Propagate(collectionId string, geolocation string, reqOrigin PropagationOrigin, isCollRecreated bool) error
}

type DeleteMetadataPropagatorImpl struct {
	serverInfo *server.ServerInfo
	httpHelper *HTTPHelper
	log        logger.Logger
}

func NewDeleteMetadataPropagator(serverInfo *server.ServerInfo, log logger.Logger) *DeleteMetadataPropagatorImpl {
	return &DeleteMetadataPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
		httpHelper: NewHTTPHelper(serverInfo, log),
	}
}

func (p *DeleteMetadataPropagatorImpl) Propagate(collectionId string, geolocation string, reqOrigin PropagationOrigin, isCollRecreated bool) error {
	p.log.Info("Starting propagation of metadata deletion", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain)
	p.log.Debug("Starting propagation of metadata deletion", "metadata_geolocation", geolocation, "request_origin", reqOrigin)

	switch reqOrigin {
	case FROM_FATHER:
		if p.log.Enabled(logger.DebugLevel) {
			p.log.Debug("Propagation origin: FROM_FATHER", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "node_children", fmt.Sprintf("%+v", p.serverInfo.Children))
		}
		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(geolocation, child.MetadataDomain) {
				p.log.Info("Forwarding metadata deletion to child", "collection_id", collectionId, "child_hostname", child.HostName, "child_geolocation", child.MetadataDomain)
				err := p.sendHttpReqToDest(child.HostName, collectionId, geolocation, isCollRecreated)
				if err != nil {
					p.log.Error("Failed to propagate metadata deletion to child", "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "child_geolocation", child.MetadataDomain, "child_hostname", child.HostName, "error", err)
					return err
				}
				break
			}
		}
	case FROM_CHILD:
		if p.log.Enabled(logger.DebugLevel) {
			p.log.Debug("Propagation origin: FROM_CHILD", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "node_father", fmt.Sprintf("%+v", p.serverInfo.Father))
		}
		if p.serverInfo.Father != nil {
			p.log.Info("Forwarding metadata deletion to father", "collection_id", collectionId, "father_hostname", p.serverInfo.Father.HostName, "father_geolocation", p.serverInfo.Father.MetadataDomain)
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, geolocation, isCollRecreated)
			if err != nil {
				p.log.Error("Failed to propagate metadata deletion to father", "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "father_geolocation", p.serverInfo.Father.MetadataDomain, "father_hostname", p.serverInfo.Father.HostName, "error", err)
				return err
			}
		}
	case FROM_LOCAL:
		if p.log.Enabled(logger.DebugLevel) {
			p.log.Debug("Propagation origin: FROM_LOCAL", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "node_children", fmt.Sprintf("%+v", p.serverInfo.Children), "node_father", fmt.Sprintf("%+v", p.serverInfo.Father))
		}
		if p.serverInfo.Father != nil {
			p.log.Info("Forwarding metadata deletion to father", "collection_id", collectionId, "father_hostname", p.serverInfo.Father.HostName, "father_geolocation", p.serverInfo.Father.MetadataDomain)
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, geolocation, isCollRecreated)
			if err != nil {
				p.log.Error("Failed to propagate metadata deletion to father", "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "father_geolocation", p.serverInfo.Father.MetadataDomain, "father_hostname", p.serverInfo.Father.HostName, "error", err)
				return err
			}
		}

		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(geolocation, child.MetadataDomain) {
				p.log.Info("Forwarding metadata deletion to child", "collection_id", collectionId, "child_hostname", child.HostName, "child_geolocation", child.MetadataDomain)
				err := p.sendHttpReqToDest(child.HostName, collectionId, geolocation, isCollRecreated)
				if err != nil {
					p.log.Error("Failed to propagate metadata deletion to child", "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "child_geolocation", child.MetadataDomain, "child_hostname", child.HostName, "error", err)
					return err
				}

				break
			}
		}
	}
	return nil
}

func (p *DeleteMetadataPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, geolocation string, isCollRecreated bool) error {
	p.log.Debug("Creating HTTP request for metadata deletion propagation", "collection_id", collectionId, "destination", dest, "metadata_geolocation", geolocation)

	req, err := p.httpHelper.CreateBasicPvtHttpRequest(http.MethodDelete, dest, EndpointDeleteMetadata, collectionId, geolocation)
	if err != nil {
		p.log.ErrorfWith("Failed to create basic HTTP request for '%s' path", []any{EndpointDeleteMetadata}, "collection_id", collectionId, "destination", dest, "metadata_geolocation", geolocation, "error", err)
		return err
	}
	req.Header.Set("X-ERMES-METADATA-DOMAIN", p.serverInfo.MetadataDomain)
	req.Header.Set("X-ERMES-COLLECTION-RECREATED", strconv.FormatBool(isCollRecreated))

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.ErrorfWith("Failed to send HTTP request for '%s' path", []any{EndpointDeleteMetadata}, "collection_id", collectionId, "destination", dest, "metadata_geolocation", geolocation, "request_headers", fmt.Sprintf("%+v", req.Header), "error", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.ErrorfWith("Unexpected status code received while contacting '%s' path", []any{EndpointCreateMetadata}, "collection_id", collectionId, "destination", dest, "metadata_geolocation", geolocation, "received_status_code", resp.Status, "expected_status_code", http.StatusOK)
		return fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, dest)
	}

	p.log.Info("Metadata deletion successfully propagated", "collection_id", collectionId)
	p.log.Debug("Metadata deletion successfully propagated", "destination", dest, "metadata_geolocation", geolocation)
	return nil
}
