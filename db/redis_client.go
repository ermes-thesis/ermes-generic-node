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

func (r *RedisClient) GetRecord(collectionId string, recordName string) (string, error) {
	return r.client.Get(r.ctx, buildKey(collectionId, recordName)).Result()
}

func (r *RedisClient) SetRecord(collectionId string, recordName string, value string) error {
	err := r.client.SAdd(r.ctx, "info:"+collectionId+":keys", recordName).Err()
	if err != nil {
		return err
	}
	return r.client.Set(r.ctx, buildKey(collectionId, recordName), value, 0).Err()
}

func (r *RedisClient) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	return r.client.MGet(r.ctx, buildKeys(collectionId, recordNames)...).Result()
}

func (r *RedisClient) RecordExists(collectionId string, recordName string) (bool, error) {
	res, err := r.client.Exists(r.ctx, buildKey(collectionId, recordName)).Result()
	return res > 0, err
}

func (r *RedisClient) DeleteRecords(collectionId string, recordNames ...string) error {
	for _, recordName := range recordNames {
		err := r.client.SRem(r.ctx, "info:"+collectionId+":keys", recordName).Err()
		if err != nil {
			return err
		}
		err = r.client.Del(r.ctx, buildKey(collectionId, recordName)).Err()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisClient) GetCollection(collectionId string) (map[string]string, []string, error) {
	//TODO: capire come usare le pipeline/transaction (probabilmente con LUA script)
	keys, err := r.client.SMembers(r.ctx, "info:"+collectionId+":keys").Result()
	if err != nil {
		return nil, nil, err
	}

	values, err := r.client.MGet(r.ctx, buildKeys(collectionId, keys)...).Result()
	if err != nil {
		return nil, nil, err
	}

	res := make(map[string]string, len(keys))
	for i, key := range keys {
		res[key] = values[i].(string)
	}

	groups, err := r.client.SMembers(r.ctx, "info:"+collectionId+":groups").Result()
	if err != nil {
		return nil, nil, err
	}

	return res, groups, nil
}

func (r *RedisClient) SetCollection(collectionId string, records map[string]string, groups []string) error {
	err := r.client.Set(r.ctx, "info:"+collectionId, "", 0).Err()
	if err != nil {
		return err
	}

	for key, value := range records {
		err := r.client.Set(r.ctx, buildKey(collectionId, key), value, 0).Err()
		if err != nil {
			return err
		}

		err = r.client.SAdd(r.ctx, "info:"+collectionId+":keys", key).Err()
		if err != nil {
			return err
		}
	}

	for _, g := range groups {
		err := r.client.SAdd(r.ctx, "info:"+collectionId+":groups", g).Err()
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RedisClient) CollectionExists(collectionId string) (bool, error) {
	res, err := r.client.Exists(r.ctx, "info:"+collectionId).Result()
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func (r *RedisClient) DeleteCollection(collectionId string) error {
	keys, err := r.client.SMembers(r.ctx, "info:"+collectionId+":keys").Result()
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		err = r.client.Del(r.ctx, buildKeys(collectionId, keys)...).Err()
		if err != nil {
			return err
		}
	}

	err = r.client.Del(r.ctx, "info:"+collectionId, "info:"+collectionId+":keys", "info:"+collectionId+":groups").Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisClient) SetMetadata(collectionId string, metadata Metadata) error {
	return r.client.HSet(r.ctx, "meta:"+collectionId, metadata.toList()...).Err()
}

func (r *RedisClient) GetMetadata(key string) (*Metadata, error) {
	res, err := r.client.HGetAll(r.ctx, "meta:"+key).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return fromMap(res)
}

func (r *RedisClient) DeleteMetadata(collectionId string) error {
	return r.client.Del(r.ctx, "meta:"+collectionId).Err()
}

func (r *RedisClient) AddGroups(collectionId string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	return r.client.SAdd(r.ctx, "info:"+collectionId+":groups", m...).Err()
}

func (r *RedisClient) IsInGroups(key string, members ...string) (bool, error) {
	if len(members) == 0 {
		return false, nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	res, err := r.client.SMIsMember(r.ctx, "info:"+key+":groups", m...).Result()

	return slices.Contains(res, true), err
}

func (r *RedisClient) RemoveGroups(key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	return r.client.SRem(r.ctx, "info:"+key+":groups", m...).Err()
}

func (r *RedisClient) CreateIndex(customMetadataKeys ...string) error {
	fields := make([]*redis.FieldSchema, len(customMetadataKeys))
	for i, key := range customMetadataKeys {
		fields[i] = &redis.FieldSchema{
			FieldName: key,
			FieldType: redis.SearchFieldTypeText,
		}
	}

	return r.client.FTCreate(r.ctx, "metadata_index", &redis.FTCreateOptions{
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
	}}, fields...)...).Err()
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

	return r.client.FTAlter(r.ctx, "metadata_index", true, fields).Err()
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}

func buildKey(collectionId string, recordName string) string {
	return "data:" + collectionId + ":" + recordName
}

func buildKeys(collectionId string, recordNames []string) []string {
	keys := slices.Clone(recordNames)
	for i, name := range keys {
		keys[i] = buildKey(collectionId, name)
	}
	return keys
}
