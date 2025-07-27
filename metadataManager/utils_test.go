package metadataManager

import (
	"db"
	"io"
	"server/info"
)

func CreateMetadataManagerWithMocks(client db.GenericDB, serverInfo *info.ServerInfo, logger io.Writer, qPropagator QueryPropagator, mPropagator MetadataPropagator) MetadataManager {
	return MetadataManagerImpl{
		redis:  client,
		logger: logger,
		templateCache: map[string]string{
			"private_data_query": `
				{
				  "SELECT": "ALL",
				  "AS": "private_data",
				  "WHERE": {
					"AND": [
					  {
						"field": "owner",
						"eq": "12345"
					  },
					  {
						"field": "name",
						"eq": "<name>"
					  }
					]
				  }
				}
		`,
			"gt_data_query": `
				{
				  "SELECT": "ALL",
				  "AS": "gt_data",
				  "WHERE": {
					"AND": [
					  {
						"field": "owner",
						"eq": "12345"
					  },
					  {
						"field": "name",
						"eq": "DATI_PRIVATE"
					  },
					  {
						"field": "created_at",
						"gt": 1745747999
					  }
					]
				  }
				}
		`,
		},
		serverInfo:         serverInfo,
		queryPropagator:    qPropagator,
		metadataPropagator: mPropagator,
		customMetadataKeys: []string{},
	}
}
