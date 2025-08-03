package db

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"metadataManager/utils"
	"slices"
	"strconv"
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
	exists, err := r.CollectionExists(collectionId)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("collection does not exist")
	}

	err = r.client.SAdd(r.ctx, "info:"+collectionId+":keys", recordName).Err()
	if err != nil {
		return err
	}
	return r.client.Set(r.ctx, buildKey(collectionId, recordName), value, 0).Err()
}

func (r *RedisClient) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	return r.client.MGet(r.ctx, buildKeys(collectionId, recordNames)...).Result()
}

func (r *RedisClient) SetRecords(collectionId string, recordNames []string, recordValues []string) error {
	exists, err := r.CollectionExists(collectionId)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("collection does not exist")
	}

	if len(recordNames) != len(recordValues) {
		return errors.New("recordNames and recordValue must be the same length")
	}

	anyConvertedNames := make([]any, len(recordNames))
	inlineRecords := make([]any, len(recordNames)*2)
	for i := 0; i < len(recordNames); i++ {
		anyConvertedNames[i] = recordNames[i]
		inlineRecords[i*2] = buildKey(collectionId, recordNames[i])
		inlineRecords[i*2+1] = recordValues[i]
	}

	err = r.client.SAdd(r.ctx, "info:"+collectionId+":keys", anyConvertedNames...).Err()
	if err != nil {
		return err
	}
	return r.client.MSet(r.ctx, inlineRecords...).Err()
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
	const maxRetries = 5

	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"

	var (
		keys   []string
		groups []string
		result map[string]string
	)

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			var err error
			keys, err = tx.SMembers(r.ctx, keysKey).Result()
			if err != nil {
				return err
			}

			groups, err = tx.SMembers(r.ctx, groupsKey).Result()
			if err != nil {
				return err
			}

			cmds, err := tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.MGet(r.ctx, buildKeys(collectionId, keys)...)
				return nil
			})
			if err != nil {
				return err
			}

			mgetCmd := cmds[0].(*redis.SliceCmd)
			values, err := mgetCmd.Result()
			if err != nil {
				return err
			}

			result = make(map[string]string, len(keys))
			for i, key := range keys {
				result[key] = values[i].(string)
			}

			return nil
		}, keysKey, groupsKey)

		if err == nil {
			return result, groups, nil
		}

		if errors.Is(err, redis.TxFailedErr) {
			continue
		}

		return nil, nil, err
	}

	return nil, nil, errors.New("transaction retries exhausted")
}

func (r *RedisClient) SetCollection(collectionId string, records map[string]string, groups []string) error {
	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"

	_, err := r.client.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(r.ctx, "info:"+collectionId, "", 0)

		for key, value := range records {
			pipe.Set(r.ctx, buildKey(collectionId, key), value, 0)
			pipe.SAdd(r.ctx, keysKey, key)
		}

		for _, g := range groups {
			pipe.SAdd(r.ctx, groupsKey, g)
		}

		return nil
	})

	return err
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

func (r *RedisClient) CollectionExists(collectionId string) (bool, error) {
	res, err := r.client.Exists(r.ctx, "info:"+collectionId).Result()
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func (r *RedisClient) GetMetadata(collectionId string) (*Metadata, error) {
	res, err := r.client.HGetAll(r.ctx, "meta:"+collectionId).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return fromMap(res)
}

func (r *RedisClient) SetMetadata(collectionId string, metadata Metadata) error {
	return r.client.HSet(r.ctx, "meta:"+collectionId, metadata.toList()...).Err()
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

func (r *RedisClient) IsInGroups(collectionId string, members ...string) (bool, error) {
	if len(members) == 0 {
		return false, nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	res, err := r.client.SMIsMember(r.ctx, "info:"+collectionId+":groups", m...).Result()

	return slices.Contains(res, true), err
}

func (r *RedisClient) RemoveGroups(collectionId string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	m := make([]any, len(members))
	for i, member := range members {
		m[i] = member
	}

	return r.client.SRem(r.ctx, "info:"+collectionId+":groups", m...).Err()
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

func (r *RedisClient) GetMemoryStats() (*MemStats, error) {
	stats, err := r.client.Do(r.ctx, "MEMORY", "STATS").Result()
	if err != nil {
		return nil, err
	}

	statsArr := stats.([]interface{})

	statsMap := make(map[string]any)
	for i := 0; i < len(statsArr); i += 2 {
		key := statsArr[i].(string)
		value := statsArr[i+1]
		statsMap[key] = value
	}

	datasetPerc, err := strconv.ParseFloat(statsMap["dataset.percentage"].(string), 64)
	if err != nil {
		return nil, err
	}
	memStats := MemStats{
		AllocationStats: AllocationStats{
			UsedMemoryPeak:              uint64(statsMap["peak.allocated"].(int64)),
			CurrentUsedMemory:           uint64(statsMap["total.allocated"].(int64)),
			UsedMemoryOverhead:          uint64(statsMap["overhead.total"].(int64)),
			UsedMemoryStartup:           uint64(statsMap["startup.allocated"].(int64)),
			UsedMemoryDataset:           uint64(statsMap["dataset.bytes"].(int64)),
			UsedMemoryDatasetPercentage: datasetPerc,
		},
		ReplicationBacklogSize: uint64(statsMap["replication.backlog"].(int64)),
		LuaScriptsCacheSize:    uint64(statsMap["lua.caches"].(int64)),
		KeysCount:              uint64(statsMap["keys.count"].(int64)),
		BytesKeysRatio:         uint64(statsMap["keys.bytes-per-key"].(int64)),
	}

	return &memStats, nil
}

func (r *RedisClient) GetCollectionMemoryUsage(collectionId string) (uint64, error) {
	keys, err := r.client.SMembers(r.ctx, "info:"+collectionId+":keys").Result()
	if err != nil {
		return 0, err
	}

	totalMemory := uint64(0)
	for _, key := range keys {
		memUsage, err := r.client.MemoryUsage(r.ctx, buildKey(collectionId, key)).Result()
		if err != nil {
			return 0, err
		}

		totalMemory += uint64(memUsage)
	}

	keysRecordSize, err := r.client.MemoryUsage(r.ctx, "info:"+collectionId+":keys").Result()
	if err != nil {
		return 0, err
	}
	groupsRecordSize, err := r.client.MemoryUsage(r.ctx, "info:"+collectionId+":groups").Result()
	if err != nil {
		return 0, err
	}
	existingRecordSize, err := r.client.MemoryUsage(r.ctx, "info:"+collectionId).Result()
	if err != nil {
		return 0, err
	}
	totalMemory += uint64(keysRecordSize) + uint64(groupsRecordSize) + uint64(existingRecordSize)

	return totalMemory, nil
}

func (r *RedisClient) GetRecordMemoryUsage(collectionId string, recordName string) (uint64, error) {
	memUsage, err := r.client.MemoryUsage(r.ctx, buildKey(collectionId, recordName)).Result()
	return uint64(memUsage), err
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
