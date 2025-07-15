package lookupManager

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"lookupManager/utils"
	"strings"
)

type LookupManager interface {
	ExecuteQueries(query map[string]map[string]any) (map[string][]CollectionInfo, error)
}

type LookupManagerImpl struct {
	redis         *redis.Client
	logger        io.Writer
	templateCache map[string]string
	redisIndex    string
}

func NewLookupManager(redis *redis.Client, logger io.Writer) LookupManager {
	return &LookupManagerImpl{
		redis:  redis,
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
						"eq": "<ownerID>"
					  },
					  {
						"field": "name",
						"eq": "DATI_PRIVATE"
					  }
					]
				  }
				}
		`,
		},
		redisIndex: "metadata_index",
	}
}

type CollectionInfo struct {
	Id       string
	Metadata map[string]string
}

func (l LookupManagerImpl) ExecuteQueries(queryParams map[string]map[string]any) (map[string][]CollectionInfo, error) {
	result := make(map[string][]CollectionInfo)
	for queryName, params := range queryParams {
		if q, ok := l.templateCache[queryName]; ok {
			var query utils.Query
			if err := json.Unmarshal([]byte(q), &query); err != nil {
				fmt.Fprintf(l.logger, "Error unmarshalling queryParams template: %v\n", err)
				return nil, err
			}

			err := query.ReplacePlaceholder(params)
			if err != nil {
				fmt.Fprintf(l.logger, "Error replacing placeholders in queryParams: %v\n", err)
				return nil, err
			}

			if err := query.Validate(); err != nil {
				fmt.Fprintf(l.logger, "Error validating queryParams: %v\n", err)
				return nil, err
			}

			redisQuery, err := query.ToRedisSearchCommand()
			if err != nil {
				fmt.Fprintf(l.logger, "Error converting queryParams to Redis command: %v\n", err)
				return nil, err
			}
			fmt.Fprintf(l.logger, "Executing Redis command: %s\n", redisQuery)

			res, err := l.redis.FTSearch(context.Background(), l.redisIndex, redisQuery).Result()
			if err != nil {
				fmt.Fprintf(l.logger, "Error executing Redis command: %v\n", err)
				return nil, err
			}
			fmt.Fprintf(l.logger, "Redis command result: %v\n", res)

			for _, doc := range res.Docs {
				collId, _ := strings.CutPrefix(doc.ID, "meta:")

				collectionInfo := CollectionInfo{
					Id:       collId,
					Metadata: doc.Fields,
				}

				result[query.As] = append(result[query.As], collectionInfo)
			}

		}
	}
	return result, nil
}
