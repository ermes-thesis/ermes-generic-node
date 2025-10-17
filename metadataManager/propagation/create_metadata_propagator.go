package propagation

import (
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"net/http"
	"server/info"
	"strings"
)

type CreateMetadataPropagator interface {
	Propagate(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error
}

type CreateMetadataPropagatorImpl struct {
	serverInfo *info.ServerInfo
	httpHelper *HTTPHelper
	log        *logger.Logger
}

func NewCreateMetadataPropagator(serverInfo *info.ServerInfo, log *logger.Logger) *CreateMetadataPropagatorImpl {
	return &CreateMetadataPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
		httpHelper: NewHTTPHelper(serverInfo, log),
	}
}

func (p *CreateMetadataPropagatorImpl) Propagate(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error {
	p.log.Info("Starting to propagating groups update request...")

	switch reqOrigin {
	case FROM_FATHER:
		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				err := p.sendHttpReqToDest(child.HostName, collectionId, metadata)
				if err != nil {
					p.log.Errorf("Error sending request to child: %v\n", err)
					return err
				}

				break
			}
		}
	case FROM_CHILD:
		if p.serverInfo.Father != nil {
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, metadata)
			if err != nil {
				p.log.Errorf("Error sending request to father : %v\n", err)
				return err
			}
		}
	case FROM_LOCAL:
		if p.serverInfo.Father != nil {
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, metadata)
			if err != nil {
				p.log.Errorf("Error sending request to father : %v\n", err)
				return err
			}
		}

		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				err := p.sendHttpReqToDest(child.HostName, collectionId, metadata)
				if err != nil {
					p.log.Errorf("Error sending request to child : %v\n", err)
					return err
				}

				break
			}
		}
	}
	return nil
}

func (p *CreateMetadataPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, metadata model.Metadata) error {
	req, err := p.httpHelper.CreateBasicPvtHttpRequest(http.MethodPost, dest, EndpointCreateMetadata, collectionId, metadata)
	if err != nil {
		p.log.Errorf("Error creating request: %v\n", err)
		return err
	}
	req.Header.Set("X-ERMES-METADATA-DOMAIN", p.serverInfo.MetadataDomain)

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.Errorf("[%s] Error sending request: %v\n", p.serverInfo.MetadataDomain, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("error when contacting the node: %s", resp.Status)
	}
	return nil
}
