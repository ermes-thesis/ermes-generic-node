package propagation

import (
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"net/http"
	"server"
	"strings"
)

type CreateMetadataPropagator interface {
	Propagate(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error
}

type CreateMetadataPropagatorImpl struct {
	serverInfo *server.ServerInfo
	httpHelper *HTTPHelper
	log        logger.Logger
}

func NewCreateMetadataPropagator(serverInfo *server.ServerInfo, log logger.Logger) *CreateMetadataPropagatorImpl {
	return &CreateMetadataPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
		httpHelper: NewHTTPHelper(serverInfo, log),
	}
}

func (p *CreateMetadataPropagatorImpl) Propagate(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error {
	p.log.Info("Starting propagation of metadata creation", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain)
	p.log.Debug("Starting propagation of metadata creation", "metadata", fmt.Sprintf("%v", metadata), "request_origin", reqOrigin)

	switch reqOrigin {
	case FROM_FATHER:
		p.log.Debug("Propagation origin: FROM_FATHER", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "node_children", fmt.Sprintf("%+v", p.serverInfo.Children))
		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				p.log.Info("Forwarding metadata creation to child", "collection_id", collectionId, "child_hostname", child.HostName, "child_geolocation", child.MetadataDomain)
				err := p.sendHttpReqToDest(child.HostName, collectionId, metadata)
				if err != nil {
					p.log.Error("Failed to propagate metadata creation to child", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata), "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "child_geolocation", child.MetadataDomain, "child_hostname", child.HostName, "error", err)
					return err
				}

				break
			}
		}
	case FROM_CHILD:
		p.log.Debug("Propagation origin: FROM_CHILD", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "node_father", fmt.Sprintf("%+v", p.serverInfo.Father))
		if p.serverInfo.Father != nil {
			p.log.Info("Forwarding metadata creation to father", "collection_id", collectionId, "father_hostname", p.serverInfo.Father.HostName, "father_geolocation", p.serverInfo.Father.MetadataDomain)
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, metadata)
			if err != nil {
				p.log.Error("Failed to propagate metadata to father", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata), "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "father_geolocation", p.serverInfo.Father.MetadataDomain, "father_hostname", p.serverInfo.Father.HostName, "error", err)
				return err
			}
		}
	case FROM_LOCAL:
		p.log.Debug("Propagation origin: FROM_LOCAL", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "node_children", fmt.Sprintf("%+v", p.serverInfo.Children), "node_father", fmt.Sprintf("%+v", p.serverInfo.Father))
		if p.serverInfo.Father != nil {
			p.log.Info("Forwarding metadata creation to father", "collection_id", collectionId, "father_hostname", p.serverInfo.Father.HostName, "father_geolocation", p.serverInfo.Father.MetadataDomain)
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, metadata)
			if err != nil {
				p.log.Error("Failed to propagate metadata to father", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata), "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "father_geolocation", p.serverInfo.Father.MetadataDomain, "father_hostname", p.serverInfo.Father.HostName, "error", err)
				return err
			}
		}

		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				p.log.Info("Forwarding metadata creation to child", "collection_id", collectionId, "child_hostname", child.HostName, "child_geolocation", child.MetadataDomain)
				err := p.sendHttpReqToDest(child.HostName, collectionId, metadata)
				if err != nil {
					p.log.Error("Failed to propagate metadata creation to child", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata), "request_origin", reqOrigin, "node_geolocation", p.serverInfo.MetadataDomain, "child_geolocation", child.MetadataDomain, "child_hostname", child.HostName, "error", err)
					return err
				}

				break
			}
		}
	}
	return nil
}

func (p *CreateMetadataPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, metadata model.Metadata) error {
	p.log.Debug("Creating HTTP request for metadata propagation", "collection_id", collectionId, "destination", dest, "metadata", fmt.Sprintf("%+v", metadata))

	req, err := p.httpHelper.CreateBasicPvtHttpRequest(http.MethodPost, dest, EndpointCreateMetadata, collectionId, metadata)
	if err != nil {
		p.log.ErrorfWith("Failed to create basic HTTP request for '%s' path", []any{EndpointCreateMetadata}, "collection_id", collectionId, "destination", dest, "metadata", fmt.Sprintf("%+v", metadata), "error", err)
		return err
	}
	req.Header.Set("X-ERMES-METADATA-DOMAIN", p.serverInfo.MetadataDomain)

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.ErrorfWith("Failed to send HTTP request for '%s' path", []any{EndpointCreateMetadata}, "collection_id", collectionId, "destination", dest, "metadata", fmt.Sprintf("%+v", metadata), "request_headers", fmt.Sprintf("%+v", req.Header), "error", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		p.log.ErrorfWith("Unexpected status code received while contacting '%s' path", []any{EndpointCreateMetadata}, "collection_id", collectionId, "destination", dest, "metadata", fmt.Sprintf("%+v", metadata), "received_status_code", resp.Status, "expected_status_code", http.StatusCreated)
		return fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, dest)
	}

	p.log.Info("Metadata creation successfully propagated", "collection_id", collectionId)
	p.log.Debug("Metadata creation successfully propagated", "destination", dest, "metadata", fmt.Sprintf("%+v", metadata))
	return nil
}
