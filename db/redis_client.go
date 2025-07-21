package db

import (
	"context"
	"github.com/redis/go-redis/v9"
	"metadataManager/utils"
	"slices"
	"strings"
)

type RedisClient struct {
	client *redis.Client
	ctx    context.Context
}

func (r *RedisClient) Get(key string) (string, error) {
	return r.client.Get(r.ctx, key).Result()
}

func (r *RedisClient) Set(key string, value string) error {
	return r.client.Set(r.ctx, key, value, 0).Err()
}

func (r *RedisClient) MGet(keys ...string) ([]any, error) {
	return r.client.MGet(r.ctx, keys...).Result()
}

func (r *RedisClient) Exists(key string) (bool, error) {
	res, err := r.client.Exists(r.ctx, key).Result()
	return res > 0, err
}

func (r *RedisClient) Del(key ...string) error {
	return r.client.Del(r.ctx, key...).Err()
}

func (r *RedisClient) SetMetadata(collectionId string, metadata Metadata) error {
	return r.client.HSet(r.ctx, "meta:"+collectionId, metadata.toList()...).Err()

}

func (r *RedisClient) GetMetadata(collectionId string, field string) (string, error) {
	return r.client.HGet(r.ctx, "meta:"+collectionId, field).Result()
}

func (r *RedisClient) GetAllMetadata(key string) (*Metadata, error) {
	res, err := r.client.HGetAll(r.ctx, "meta:"+key).Result()
	if err != nil {
		return nil, err
	}
	return fromMap(res)
}

func (r *RedisClient) AddGroup(collectionId string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	return r.client.SAdd(r.ctx, "data:"+collectionId+":groups", m...).Err()
}

func (r *RedisClient) IsInGroups(key string, members ...string) (bool, error) {
	if len(members) == 0 {
		return false, nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	res, err := r.client.SMIsMember(r.ctx, "data:"+key+":groups", m...).Result()

	return slices.Contains(res, true), err
}

func (r *RedisClient) RemoveGroup(key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	return r.client.SRem(r.ctx, "data:"+key+":groups", m...).Err()
}

func (r *RedisClient) CreateIndex(customMetadataKeys ...string) error {
	fields := make([]*redis.FieldSchema, len(customMetadataKeys))
	for i, key := range customMetadataKeys {
		fields[i] = &redis.FieldSchema{
			FieldName: key,
			FieldType: redis.SearchFieldTypeText,
		}
	}

	r.client.FTCreate(r.ctx, "metadata_index", &redis.FTCreateOptions{
		OnHash: true,
		Prefix: []any{1, "meta:"},
	}, append([]*redis.FieldSchema{{
		FieldName: "name",
		FieldType: redis.SearchFieldTypeText,
	}, {
		FieldName: "owner",
		FieldType: redis.SearchFieldTypeText,
	}, {
		FieldName: "created_at",
		FieldType: redis.SearchFieldTypeNumeric,
	}, {
		FieldName: "groups",
		FieldType: redis.SearchFieldTypeTag,
	}}, fields...)...)
	return nil
}

func (r *RedisClient) SearchOnMetadata(query utils.Query, userGroups []string) ([]CollectionInfo, error) {
	redisQuery, err := query.ToRedisSearchCommand()
	if err != nil {
		return nil, err
	}
	redisQuery = redisQuery + " @groups:{" + strings.Join(userGroups, " | ") + "}"

	res, err := r.client.FTSearch(context.Background(), "metadata_index", redisQuery).Result()
	if err != nil {
		return nil, err
	}

	var result []CollectionInfo
	for _, doc := range res.Docs {
		collId, _ := strings.CutPrefix(doc.ID, "meta:")

		metadata, err := fromMap(doc.Fields)
		if err != nil {
			return nil, err
		}

		collectionInfo := CollectionInfo{
			Id:       collId,
			Metadata: *metadata,
		}

		result = append(result, collectionInfo)
	}

	return result, nil
}

func (r *RedisClient) DropIndex() error {
	return r.client.FTDropIndex(r.ctx, "metadata_index").Err()
}

func (r *RedisClient) AlterIndex(customMetadataKeys ...string) error {
	fields := make([]any, len(customMetadataKeys)*2)
	for i := 0; i < len(fields); i += 2 {
		fields[i] = customMetadataKeys[i/2]
		fields[i+1] = "TEXT"
	}

	return r.client.FTAlter(r.ctx, "metadata_index", true, append([]any{"SCHEMA", "ADD"}, fields...)).Err()
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}
