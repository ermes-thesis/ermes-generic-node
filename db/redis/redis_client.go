package redis

import (
	"config"
	"context"
	"db"
	"errors"
	"metadataManager/utils"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
	ctx    context.Context
	log    *logger.Logger
}

func NewRedisClient(config config.RedisConfig, ctx context.Context, log *logger.Logger) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.Address + ":" + config.Port,
		Password: config.Password,
		DB:       config.DB,
		Protocol: config.Protocol,
	})
	return &RedisClient{client: rdb, ctx: ctx, log: log}
}

func (r *RedisClient) GetRecord(collectionId string, recordName string) (string, error) {
	return r.client.Get(r.ctx, buildKey(collectionId, recordName)).Result()
}

func (r *RedisClient) SetRecord(collectionId string, recordName string, value string) error {
	infoKey := "info:" + collectionId
	dataKey := buildKey(collectionId, recordName)
	keysKey := "info:" + collectionId + ":keys"

	err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
		exists, err := tx.Exists(r.ctx, infoKey).Result()
		if err != nil {
			return err
		}
		if exists == 0 {
			return errors.New("collection does not exist")
		}

		_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
			pipe.SAdd(r.ctx, keysKey, recordName)
			pipe.Set(r.ctx, dataKey, value, 0)
			return nil
		})

		return err
	}, infoKey)

	return err
}

func (r *RedisClient) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	return r.client.MGet(r.ctx, buildKeys(collectionId, recordNames)...).Result()
}
func (r *RedisClient) SetRecords(collectionId string, recordNames []string, recordValues []string) error {
	if len(recordNames) != len(recordValues) {
		return errors.New("recordNames and recordValues must be the same length")
	}

	infoKey := "info:" + collectionId
	keysKey := "info:" + collectionId + ":keys"

	return r.client.Watch(r.ctx, func(tx *redis.Tx) error {
		exists, err := tx.Exists(r.ctx, infoKey).Result()
		if err != nil {
			return err
		}
		if exists == 0 {
			return errors.New("collection does not exist")
		}

		_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
			anyConvertedNames := make([]any, len(recordNames))
			inlineRecords := make([]any, len(recordNames)*2)
			for i, name := range recordNames {
				anyConvertedNames[i] = name
				inlineRecords[i*2] = buildKey(collectionId, recordNames[i])
				inlineRecords[i*2+1] = recordValues[i]
			}
			pipe.SAdd(r.ctx, keysKey, anyConvertedNames...)
			pipe.MSet(r.ctx, inlineRecords...)

			return nil
		})

		return err
	}, infoKey)
}

func (r *RedisClient) RecordExists(collectionId string, recordName string) (bool, error) {
	res, err := r.client.Exists(r.ctx, buildKey(collectionId, recordName)).Result()
	return res > 0, err
}

func (r *RedisClient) DeleteRecords(collectionId string, recordNames ...string) error {
	keysKey := "info:" + collectionId + ":keys"

	_, err := r.client.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
		for _, recordName := range recordNames {
			dataKey := buildKey(collectionId, recordName)
			pipe.SRem(r.ctx, keysKey, recordName)
			pipe.Del(r.ctx, dataKey)
		}
		return nil
	})

	return err
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
	keysKey := "info:" + collectionId + ":keys"
	infoKey := "info:" + collectionId
	groupsKey := "info:" + collectionId + ":groups"

	err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
		keys, err := tx.SMembers(r.ctx, keysKey).Result()
		if err != nil {
			return err
		}

		dataKeys := buildKeys(collectionId, keys)
		allKeysToDelete := append(dataKeys, infoKey, keysKey, groupsKey)

		_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(r.ctx, allKeysToDelete...)
			return nil
		})

		return err
	}, keysKey)

	return err
}

func (r *RedisClient) CollectionExists(collectionId string) (bool, error) {
	res, err := r.client.Exists(r.ctx, "info:"+collectionId).Result()
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func (r *RedisClient) GetMetadata(collectionId string) (*model.Metadata, error) {
	res, err := r.client.HGetAll(r.ctx, "meta:"+collectionId).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return model.FromMap(res)
}

func (r *RedisClient) SetMetadata(collectionId string, metadata model.Metadata) error {
	return r.client.HSet(r.ctx, "meta:"+collectionId, metadata.ToList()...).Err()
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

func (r *RedisClient) SearchOnMetadata(query model.QueryTemplate, userGroups []string) ([]utils.CollectionInfo, error) {
	redisQuery, err := query.ToRedisSearchCommand()
	if err != nil {
		return nil, err
	}
	redisQuery = redisQuery + " @groups:{" + strings.Join(userGroups, " | ") + "}"

	res, err := r.client.FTSearch(context.Background(), "metadata_index", redisQuery).Result()
	if err != nil {
		return nil, err
	}

	var result []utils.CollectionInfo
	for _, doc := range res.Docs {
		collId, _ := strings.CutPrefix(doc.ID, "meta:")

		metadata, err := model.FromMap(doc.Fields)
		if err != nil {
			return nil, err
		}

		collectionInfo := utils.CollectionInfo{
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

func strToInt(s string) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func parseRedisInfo(info string) map[string]string {
	lines := strings.Split(info, "\r\n")
	result := make(map[string]string, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]
			result[key] = value
		}
	}

	return result
}

func (r *RedisClient) GetMaxMemory() (uint64, error) {
	stats, err := r.client.ConfigGet(r.ctx, "maxmemory").Result()
	if err != nil {
		return 0, err
	}
	if val, ok := stats["maxmemory"]; ok {
		return strToInt(val), nil
	}
	return strToInt(stats["maxmemory"]), nil
}

func (r *RedisClient) GetAvailableMemory() (uint64, error) {
	maxMem, err := r.GetMaxMemory()
	if err != nil {
		return 0, err
	}
	if maxMem == 0 {
		return ^uint64(0), nil
	}

	stats, err := r.GetMemoryStats()
	if err != nil {
		return 0, err
	}
	return maxMem - stats.CurrentUsedMemory, nil
}

func (r *RedisClient) GetMemoryStats() (*db.MemStats, error) {
	stats, err := r.client.Info(r.ctx, "MEMORY", "KEYSPACE").Result()
	if err != nil {
		return nil, err
	}

	statsMap := parseRedisInfo(stats)

	re := regexp.MustCompile(`keys=(\d+)`)
	match := re.FindStringSubmatch(statsMap["db0"])
	if len(match) != 2 {
		return nil, errors.New("keys not found")
	}

	datasetPerc, err := strconv.ParseFloat(statsMap["used_memory_dataset_perc"][:len(statsMap["used_memory_dataset_perc"])-1], 64)
	memStats := db.MemStats{
		AllocationStats: db.AllocationStats{
			UsedMemoryPeak:              strToInt(statsMap["used_memory_peak"]),
			CurrentUsedMemory:           strToInt(statsMap["used_memory"]),
			UsedMemoryOverhead:          strToInt(statsMap["used_memory_overhead"]),
			UsedMemoryStartup:           strToInt(statsMap["used_memory_startup"]),
			UsedMemoryDataset:           strToInt(statsMap["used_memory_dataset"]),
			UsedMemoryDatasetPercentage: datasetPerc,
		},
		ReplicationBacklogSize: strToInt(statsMap["mem_replication_backlog"]),
		LuaScriptsCacheSize:    strToInt(statsMap["used_memory_scripts"]),
		KeysCount:              strToInt(match[1]),
		BytesKeysRatio:         strToInt(statsMap["used_memory_dataset"]) / strToInt(match[1]),
	}

	return &memStats, err
}

func (r *RedisClient) GetCollectionMemoryUsage(collectionId string) (uint64, error) {
	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"
	infoKey := "info:" + collectionId

	var totalMemory uint64

	err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
		keys, err := tx.SMembers(r.ctx, keysKey).Result()
		if err != nil {
			return err
		}

		totalMemory = 0

		for _, key := range keys {
			memUsage, err := tx.MemoryUsage(r.ctx, buildKey(collectionId, key)).Result()
			if err != nil {
				return err
			}
			totalMemory += uint64(memUsage)
		}

		metaKeys := []string{keysKey, groupsKey, infoKey}
		for _, metaKey := range metaKeys {
			memUsage, err := tx.MemoryUsage(r.ctx, metaKey).Result()
			if err != nil {
				return err
			}
			totalMemory += uint64(memUsage)
		}

		return nil
	}, keysKey)

	if err != nil {
		return 0, err
	}

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
