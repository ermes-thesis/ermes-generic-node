package metadataManager

import (
	"caches"
	"db"
	"io"
	"server/info"
)

func CreateMetadataManagerWithMocks(client db.GenericDB, serverInfo *info.ServerInfo, logger io.Writer, qPropagator QueryPropagator, mPropagator MetadataPropagator, fnCache caches.FunctionCache) MetadataManager {
	return &MetadataManagerImpl{
		redis:              client,
		logger:             logger,
		templateCache:      fnCache,
		serverInfo:         serverInfo,
		queryPropagator:    qPropagator,
		metadataPropagator: mPropagator,
		customMetadataKeys: []string{},
	}
}
