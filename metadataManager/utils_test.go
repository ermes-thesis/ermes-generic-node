package metadataManager

import (
	"caches"
	"db"
	"server/info"

	"github.com/ermes-thesis/ermes-common/logger"
)

func CreateMetadataManagerWithMocks(client db.GenericDB, serverInfo *info.ServerInfo, logger *logger.Logger, qPropagator QueryPropagator, mPropagator CreateMetadataPropagator, fnCache caches.FunctionCache) MetadataManager {
	return &MetadataManagerImpl{
		redis:                    client,
		log:                      logger,
		templateCache:            fnCache,
		serverInfo:               serverInfo,
		queryPropagator:          qPropagator,
		createMetadataPropagator: mPropagator,
		customMetadataKeys:       []string{},
	}
}
