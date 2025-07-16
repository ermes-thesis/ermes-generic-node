package metadataManager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"metadataManager/utils"
	"net/http"
	"server/info"
	"strings"
)

type MetadataManager interface {
	CreateMetadata(collectionId string, metadata map[string]any) error
	ExecuteQueries(query map[string]map[string]any, groups []string) (map[string][]CollectionInfo, error)
}

type MetadataManagerImpl struct {
	redis         *redis.Client
	logger        io.Writer
	templateCache map[string]string
	redisIndex    string
	serverInfo    *info.Serverinfo
}

func NewMetadataManager(redis *redis.Client, serverInfo *info.Serverinfo, logger io.Writer) MetadataManager {
	return &MetadataManagerImpl{
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
		redisIndex: "metadata_index",
		serverInfo: serverInfo,
	}
}

type CollectionInfo struct {
	Id       string
	Metadata map[string]string
}

func (l MetadataManagerImpl) CreateMetadata(collectionId string, metadata map[string]any) error {
	geolcation := metadata["geolocation"]

	if geolcation == l.serverInfo.MetadataDomain {
		//TODO: check if there are new types of custom metadata
		// and eventually call FT.ALTER INDEX to add them
		l.redis.HSet(context.Background(), "meta:"+collectionId, "name", metadata["name"], "owner", metadata["owner"], "created_at", metadata["created_at"], "groups", strings.Join(metadata["groups"].([]string), ","), "geolocation", geolcation)
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		fmt.Fprintf(l.logger, "Error marshalling metadata: %v\n", err)
		return err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%s/create_metadata/%s", l.serverInfo.Father, collectionId), bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(l.logger, "Error creating request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", l.serverInfo.Secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(l.logger, "Error sending request: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (l MetadataManagerImpl) ExecuteQueries(queryParams map[string]map[string]any, groups []string) (map[string][]CollectionInfo, error) {
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
			redisQuery = redisQuery + " @groups:{" + strings.Join(groups, " | ") + "}"
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
