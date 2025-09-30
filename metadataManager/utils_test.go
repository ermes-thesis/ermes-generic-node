package metadataManager

import (
	"caches"
	"db"
	"logger"
	"server/info"
)

func CreateMetadataManagerWithMocks(client db.GenericDB, serverInfo *info.ServerInfo, logger *logger.Logger, qPropagator QueryPropagator, mPropagator MetadataPropagator, fnCache caches.FunctionCache) MetadataManager {
	return &MetadataManagerImpl{
		redis:              client,
		log:                logger,
		templateCache:      fnCache,
		serverInfo:         serverInfo,
		queryPropagator:    qPropagator,
		metadataPropagator: mPropagator,
		customMetadataKeys: []string{},
	}
}
