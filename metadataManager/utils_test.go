package metadataManager

import (
	"caches"
	"db"
	"metadataManager/propagation"
	"server"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

func CreateMetadataManagerWithMocks(client db.GenericDB, serverInfo *server.ServerInfo, logger *logger.Logger, qPropagator propagation.QueryPropagator, mPropagator propagation.CreateMetadataPropagator, fnCache caches.FunctionCache) MetadataManager {
	return &MetadataManagerImpl{
		redis:                    client,
		log:                      logger,
		templateCache:            fnCache,
		serverInfo:               serverInfo,
		queryPropagator:          qPropagator,
		createMetadataPropagator: mPropagator,
		customMetadataKeys:       sync.Map{},
	}
}
