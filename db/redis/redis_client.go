package redis

import (
	"config"
	"context"
	"db"
	"errors"
	"fmt"
	"math/rand"
	"metadataManager/types"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
	ctx    context.Context
	log    *logger.Logger
}

const maxRetries = 10
const defaultBackoff = time.Millisecond * 10
const maxBackoff = time.Second * 1

type StaleUpdateError struct {
}

func (e *StaleUpdateError) Error() string {
	return "the update has not been applied since it was completely stale."
}

func NewRedisClient(config config.RedisConfig, ctx context.Context, log *logger.Logger) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.Address + ":" + config.Port,
		Password: config.Password,
		DB:       config.DB,
		Protocol: config.Protocol,
	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Error("Failed to connect to Redis", "configs", fmt.Sprintf("%+v", config), "error", err)
		return nil, err
	}
	log.Info("Connected to Redis successfully")
	return &RedisClient{client: rdb, ctx: ctx, log: log}, nil
}

func (r *RedisClient) GetRecord(collectionId string, recordName string) (string, error) {
	r.log.Info("GetRecord called", "collection_id", collectionId, "record_name", recordName)
	str, err := r.client.Get(r.ctx, buildKey(collectionId, recordName)).Result()
	if err != nil {
		r.log.Warn("GetRecord: Failed to get record", "collection_id", collectionId, "record_name", recordName, "error", err)
		return "", err
	}

	r.log.Info("GetRecord: Record retrieved successfully", "collection_id", collectionId, "record_name", recordName)
	r.log.Debug("GetRecord: Record retrieved successfully", "result", str)
	return str, nil
}

func (r *RedisClient) SetRecord(collectionId string, recordName string, value string, timestamp string) error {
	r.log.Info("SetRecord called", "collection_id", collectionId, "record_name", recordName, "record_value", value, "timestamp", timestamp)

	backoff := defaultBackoff

	infoKey := "info:" + collectionId
	dataKey := buildKey(collectionId, recordName)
	keysKey := "info:" + collectionId + ":keys"
	timeKey := "info:" + collectionId + ":" + recordName + ":timestamp"

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			exists, err := tx.Exists(r.ctx, infoKey).Result()
			if err != nil {
				return err
			}
			if exists == 0 {
				return errors.New("collection does not exist")
			}

			currentTimestamp, err := tx.Get(r.ctx, timeKey).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			if timestamp != "" && currentTimestamp >= timestamp {
				return &StaleUpdateError{}
			}

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.SAdd(r.ctx, keysKey, recordName)
				pipe.MSet(r.ctx, dataKey, value, timeKey, timestamp)
				return nil
			})

			return err
		}, infoKey, timeKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("SetRecord: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "record_name", recordName, "record_value", value, "timestamp", timestamp)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("SetRecord: Failed to set record", "collection_id", collectionId, "record_name", recordName, "record_value", value, "timestamp", timestamp, "error", err)
			return err
		}

		r.log.Info("SetRecord: Record set successfully", "collection_id", collectionId, "record_name", recordName, "record_value", value, "timestamp", timestamp)
		return nil
	}

	r.log.Warn("SetRecord failed due to retries exhaustion. The record has NOT been set", "collection_id", collectionId, "record_name", recordName, "value", value, "timestamp", timestamp)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	r.log.Info("GetRecords called", "collection_id", collectionId, "record_names", recordNames)
	strs, err := r.client.MGet(r.ctx, buildKeys(collectionId, recordNames)...).Result()
	if err != nil {
		r.log.Warn("GetRecords: Failed to get records", "collection_id", collectionId, "record_names", recordNames, "error", err)
		return nil, err
	}

	r.log.Info("GetRecords: Records retrieved successfully", "collection_id", collectionId, "record_names", recordNames)
	r.log.Debug("GetRecords: Records retrieved successfully", "result", strs)
	return strs, nil
}

func (r *RedisClient) SetRecords(collectionId string, recordNames []string, recordValues []string, timestamp string) error {
	r.log.Info("SetRecords called", "collection_id", collectionId, "record_names", recordNames, "record_values", recordValues, "timestamp", timestamp)

	if len(recordNames) != len(recordValues) {
		return errors.New("recordNames and recordValues must be the same length")
	}

	backoff := defaultBackoff

	infoKey := "info:" + collectionId
	keysKey := "info:" + collectionId + ":keys"

	timestampKeys := make([]string, len(recordNames))
	for i, name := range recordNames {
		timestampKeys[i] = buildTimestampKey(collectionId, name)
	}

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			exists, err := tx.Exists(r.ctx, infoKey).Result()
			if err != nil {
				return err
			}
			if exists == 0 {
				return errors.New("collection does not exist")
			}

			timestamps, err := tx.MGet(r.ctx, timestampKeys...).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			var anyConvertedNames []any
			var inlineRecords []any
			for i, name := range recordNames {
				if timestamps[i] == nil || timestamp == "" || timestamps[i].(string) < timestamp {
					anyConvertedNames = append(anyConvertedNames, name)
					inlineRecords = append(inlineRecords, buildKey(collectionId, name), recordValues[i], buildTimestampKey(collectionId, name), timestamp)
				}
			}

			if len(anyConvertedNames) == 0 {
				return &StaleUpdateError{}
			}

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.SAdd(r.ctx, keysKey, anyConvertedNames...)
				pipe.MSet(r.ctx, inlineRecords...)

				return nil
			})

			return err
		}, append(timestampKeys, infoKey)...)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("SetRecords: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "record_names", recordNames, "record_values", recordValues, "timestamp", timestamp)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("SetRecords: Failed to set records", "collection_id", collectionId, "record_names", recordNames, "record_values", recordValues, "timestamp", timestamp, "error", err)
			return err
		}

		r.log.Info("SetRecords: Records set successfully", "collection_id", collectionId, "record_names", recordNames, "record_values", recordValues, "timestamp", timestamp)
		return nil
	}

	r.log.Warn("SetRecords failed due to retries exhaustion. The records have NOT been set", "collection_id", collectionId, "record_names", recordNames, "record_values", recordValues, "timestamp", timestamp)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) RecordExists(collectionId string, recordName string) (bool, error) {
	r.log.Info("RecordExists called", "collection_id", collectionId, "record_name", recordName)

	res, err := r.client.Exists(r.ctx, buildKey(collectionId, recordName)).Result()
	if err != nil {
		r.log.Error("RecordExists: Failed assert record existence", "collection_id", collectionId, "record_name", recordName, "error", err)
		return false, err
	}

	r.log.Info("RecordExists: Record existence asserted successfully", "collection_id", collectionId, "record_name", recordName)
	r.log.Debug("RecordExists: Record existence asserted successfully", "exists", res > 0)
	return res > 0, err
}

func (r *RedisClient) DeleteRecords(collectionId string, timestamp string, recordNames ...string) error {
	r.log.Info("DeleteRecords called", "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp)

	keysKey := "info:" + collectionId + ":keys"
	infoKey := "info:" + collectionId

	backoff := defaultBackoff

	timestampKeys := buildTimestampKeys(collectionId, recordNames)

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			exists, err := tx.Exists(r.ctx, infoKey).Result()
			if err != nil {
				return err
			}
			if exists == 0 {
				return errors.New("collection does not exist")
			}

			timestamps, err := tx.MGet(r.ctx, timestampKeys...).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			var anyConvertedNames []any
			var dataKeys []string
			for i, name := range recordNames {
				if timestamps[i] == nil || timestamp == "" || timestamps[i].(string) < timestamp {
					anyConvertedNames = append(anyConvertedNames, name)
					dataKeys = append(dataKeys, buildKey(collectionId, name), buildTimestampKey(collectionId, name))
				}
			}

			_, err = r.client.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.SRem(r.ctx, keysKey, anyConvertedNames...)
				pipe.Del(r.ctx, dataKeys...)

				return nil
			})
			return err
		}, append(timestampKeys, infoKey)...)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("DeleteRecords: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("DeleteRecords: Failed to delete records", "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp, "error", err)
			return err
		}

		r.log.Info("DeleteRecords: Records deleted successfully", "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp)
		return nil
	}

	r.log.Warn("DeleteRecords failed due to retries exhaustion. The records have NOT been deleted", "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) GetCollection(collectionId string) (map[string]db.RecordValue, []string, string, uint64, error) {
	r.log.Info("GetCollection called", "collection_id", collectionId)

	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"
	groupsSequenceKey := "info:" + collectionId + ":groupsSequence"
	ownerKey := "info:" + collectionId + ":owner"

	backoff := defaultBackoff

	var (
		keys           []string
		groups         []string
		groupsSequence uint64
		values         []any
		timestamps     []any
		owner          string
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

			groupsSequence, err = tx.Get(r.ctx, groupsSequenceKey).Uint64()
			if err != nil {
				return err
			}

			owner, err = tx.Get(r.ctx, ownerKey).Result()
			if err != nil {
				return err
			}

			var keysCmd, timestampCmd *redis.SliceCmd
			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				keysCmd = pipe.MGet(r.ctx, buildKeys(collectionId, keys)...)
				timestampCmd = pipe.MGet(r.ctx, buildTimestampKeys(collectionId, keys)...)
				return nil
			})
			if err != nil {
				return err
			}

			values, err = keysCmd.Result()
			if err != nil {
				return err
			}

			timestamps, err = timestampCmd.Result()
			if err != nil {
				return err
			}

			return nil
		}, keysKey, groupsKey, groupsSequenceKey)

		if err == nil {
			result := make(map[string]db.RecordValue, len(keys))
			for i, key := range keys {
				result[key] = db.RecordValue{Value: values[i].(string), Timestamp: timestamps[i].(string)}
			}

			r.log.Info("GetCollection: Collection retrieved successfully", "collection_id", collectionId)
			r.log.Debug("GetCollection: Collection retrieved successfully", "records", fmt.Sprintf("%+v", result), "groups", groups, "owner", owner, "groups_sequence", groupsSequence)
			return result, groups, owner, groupsSequence, nil
		}

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("GetCollection: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		r.log.Error("GetCollection: Failed to get collection", "collection_id", collectionId, "error", err)
		return nil, nil, "", 0, err
	}

	r.log.Warn("GetCollection failed due to retries exhaustion. The collection has NOT been returned", "collection_id", collectionId)
	return nil, nil, "", 0, errors.New("transaction retries exhausted")
}

// Implemented like this, we allow the overwrite of a collection. If we want to avoid to overwrite a collection
// we would need to add a Watch( ..., infoKey) to check the existence of the collection
func (r *RedisClient) SetCollection(collectionId string, records map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64) error {
	r.log.Info("SetCollection called", "collection_id", collectionId, "records", fmt.Sprintf("%+v", records), "groups", groups, "owner", owner, "groupsSequence", groupsSequence)

	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"
	groupsSequenceKey := "info:" + collectionId + ":groupsSequence"
	ownerKey := "info:" + collectionId + ":owner"

	groupsAny := toListAny(groups)

	_, err := r.client.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(r.ctx, "info:"+collectionId, "", 0)
		pipe.Set(r.ctx, ownerKey, owner, 0)

		for key, value := range records {
			pipe.MSet(r.ctx, buildKey(collectionId, key), value.Value, buildTimestampKey(collectionId, key), value.Timestamp)
			pipe.SAdd(r.ctx, keysKey, key)
		}

		if len(groupsAny) > 0 {
			pipe.SAdd(r.ctx, groupsKey, groupsAny...)
		}
		pipe.Set(r.ctx, groupsSequenceKey, groupsSequence, 0)

		return nil
	})

	if err != nil {
		r.log.Error("SetCollection: Failed to set collection", "collection_id", collectionId, "records", fmt.Sprintf("%+v", records), "groups", groups, "owner", owner, "groups_sequence", groupsSequence, "error", err)
		return err
	}

	r.log.Info("SetCollection: Collection set successfully", "collection_id", collectionId, "records", fmt.Sprintf("%+v", records), "groups", groups, "owner", owner, "groups_sequence", groupsSequence)
	return nil
}

func (r *RedisClient) DeleteCollection(collectionId string) error {
	r.log.Info("DeleteCollection called", "collection_id", collectionId)

	keysKey := "info:" + collectionId + ":keys"
	infoKey := "info:" + collectionId
	groupsKey := "info:" + collectionId + ":groups"
	groupsSequenceKey := "info:" + collectionId + ":groupsSequence"
	ownerKey := "info:" + collectionId + ":owner"

	backoff := defaultBackoff

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			keys, err := tx.SMembers(r.ctx, keysKey).Result()
			if err != nil {
				return err
			}

			dataKeys := buildKeys(collectionId, keys)
			timestampKeys := buildTimestampKeys(collectionId, keys)
			allKeysToDelete := append(dataKeys, append(timestampKeys, infoKey, keysKey, groupsKey, groupsSequenceKey, ownerKey)...)

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(r.ctx, allKeysToDelete...)
				return nil
			})

			return err
		}, keysKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("DeleteCollection: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("DeleteCollection: Failed to delete collection", "collection_id", collectionId, "error", err)
			return err
		}

		r.log.Info("DeleteCollection: Collection deleted successfully", "collection_id", collectionId)
		return nil
	}

	r.log.Warn("DeleteCollection failed due to retries exhaustion. The collection has NOT been deleted", "collection_id", collectionId)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) CollectionExists(collectionId string) (bool, error) {
	r.log.Info("CollectionExists called", "collection_id", collectionId)

	res, err := r.client.Exists(r.ctx, "info:"+collectionId).Result()
	if err != nil {
		r.log.Error("CollectionExists: Failed to check collection existence", "collection_id", collectionId, "error", err)
		return false, err
	}

	r.log.Info("CollectionExists: Collection existence asserted successfully", "collection_id", collectionId)
	r.log.Debug("RecordExists: Collection existence asserted successfully", "exists", res > 0)
	return res > 0, nil
}

func (r *RedisClient) GetMetadata(collectionId string) (*model.Metadata, error) {
	r.log.Info("GetMetadata called", "collection_id", collectionId)

	res, err := r.client.HGetAll(r.ctx, "meta:"+collectionId).Result()
	if err != nil {
		r.log.Error("GetMetadata: Failed to get metadata", "collection_id", collectionId, "error", err)
		return nil, err
	}

	if len(res) == 0 {
		r.log.Warn("GetMetadata: No metadata found for collection", "collection_id", collectionId)
		return nil, fmt.Errorf("no metadata found for collection %s", collectionId)
	}
	delete(res, "groupsSequence")

	meta, err := model.FromMap(res)
	if err != nil {
		r.log.Error("GetMetadata: Failed to parse metadata", "collection_id", collectionId, "raw_metadata", res, "error", err)
		return nil, err
	}

	r.log.Info("GetMetadata: Metadata retrieved successfully", "collection_id", collectionId, "metadata", meta)
	return meta, nil
}

func (r *RedisClient) SetMetadata(collectionId string, metadata model.Metadata) error {
	r.log.Info("SetMetadata called", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata))

	metaList := append(metadata.ToList(), "groupsSequence", 0)
	err := r.client.HSet(r.ctx, "meta:"+collectionId, metaList).Err()
	if err != nil {
		r.log.Error("SetMetadata: Failed to set metadata", "collection_id", collectionId, "metadata", metadata, "error", err)
		return err
	}

	r.log.Info("SetMetadata: Metadata set successfully", "collection_id", collectionId, "metadata", metadata)
	return nil
}

func (r *RedisClient) MetadataExists(collectionId string) (bool, error) {
	r.log.Info("MetadataExists called", "collection_id", collectionId)

	res, err := r.client.Exists(r.ctx, "meta:"+collectionId).Result()
	if err != nil {
		r.log.Error("MetadataExists: Failed assert metadata existence", "collection_id", collectionId, "error", err)
		return false, err
	}

	r.log.Info("MetadataExists: Metadata existence asserted successfully", "collection_id", collectionId)
	r.log.Debug("RecordExists: Metadata existence asserted successfully", "exists", res > 0)
	return res > 0, err
}

func (r *RedisClient) IsUniqueForOwner(collectionName string, owner string) (bool, error) {
	r.log.Info("IsUniqueForOwner called", "collection_name", collectionName, "owner", owner)

	existenceKey := "exist:" + owner

	backoff := defaultBackoff

	var exists bool
	var err error
	for retries := 0; retries < maxRetries; retries++ {
		err = r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			exists, err = tx.SIsMember(r.ctx, "exist:"+owner, collectionName).Result()
			if err != nil {
				return err
			}
			if !exists {
				_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
					pipe.SAdd(r.ctx, "exist:"+owner, collectionName)
					return nil
				})
			}

			return err
		}, existenceKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("IsUniqueForOwner: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_name", collectionName, "owner", owner)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("IsUniqueForOwner: Failed to check uniqueness", "collection_name", collectionName, "owner", owner, "error", err)
			return false, err
		}

		r.log.Info("IsUniqueForOwner: Uniqueness checked successfully", "collection_name", collectionName, "owner", owner)
		r.log.Debug("IsUniqueForOwner: Uniqueness checked successfully", "is_unique", !exists)
		return !exists, nil
	}

	r.log.Warn("IsUniqueForOwner failed due to retries exhaustion. The pair (<collection_name>,<owner>) has NOT been 'declared' as existing. New calls may 'declare' it", "collection_name", collectionName, "owner", owner)
	return false, errors.New("transaction retries exhausted")
}

func (r *RedisClient) DeleteMetadata(collectionId string, deleteExistenceRecord bool) error {
	r.log.Info("DeleteMetadata called", "collection_id", collectionId, "delete_existence_record", deleteExistenceRecord)

	metadataKey := "meta:" + collectionId

	var name, owner string
	if deleteExistenceRecord {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			exists, err := tx.Exists(r.ctx, metadataKey).Result()
			if err != nil {
				r.log.Error("DeleteMetadata: Failed to check metadata key existence", "collection_id", collectionId, "delete_existence_record", deleteExistenceRecord, "error", err)
				return err
			}
			if exists == 0 {
				r.log.Warn("DeleteMetadata: Metadata key does not exist", "collection_id", collectionId)
				return fmt.Errorf("metadata key does not exist")
			}

			nameOwner, err := tx.HMGet(r.ctx, metadataKey, "name", "owner").Result()
			if err != nil {
				r.log.Error("DeleteMetadata: Failed to get metadata", "collection_id", collectionId, "delete_existence_record", deleteExistenceRecord, "error", err)
				return err
			}

			if slices.Contains(nameOwner, nil) {
				r.log.Error("DeleteMetadata: Metadata have been already deleted", "collection_id", collectionId, "delete_existence_record", deleteExistenceRecord, "owner", owner)
				return fmt.Errorf("metadata key has been already deleted")
			}
			name = nameOwner[0].(string)
			owner = nameOwner[1].(string)

			existenceKey := "exist:" + owner

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(r.ctx, metadataKey)
				pipe.SRem(r.ctx, existenceKey, name)

				return nil
			})

			return err
		}, metadataKey)

		if errors.Is(err, redis.TxFailedErr) {
			r.log.Debug("DeleteMetadata: Transaction failed due to high concurrency", "collection_name", name, "owner", owner)
			return err
		}

		if err != nil {
			r.log.Error("DeleteMetadata: Failed to check uniqueness", "collection_id", collectionId, "delete_existence_record", deleteExistenceRecord, "error", err)
			return err
		}
		return nil
	}

	err := r.client.Del(r.ctx, metadataKey).Err()
	if err != nil {
		r.log.Error("DeleteMetadata: Failed to delete metadata", "collection_id", collectionId, "delete_existence_record", deleteExistenceRecord, "error", err)
		return err
	}

	r.log.Info("DeleteMetadata: Metadata deleted successfully", "collection_id", collectionId)
	return nil
}

func (r *RedisClient) UpdateGroupsInMetadata(collectionId string, groupsToAdd []string, groupsToRemove []string) ([]string, uint64, error) {
	r.log.Info("UpdateGroupsInMetadata called", "collection_id", collectionId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)

	if len(groupsToRemove) == 0 && len(groupsToAdd) == 0 {
		return nil, 0, nil
	}

	metadataKey := "meta:" + collectionId

	backoff := defaultBackoff

	var groups []string
	var seq uint64

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			var seqCmd *redis.IntCmd

			exists, err := tx.Exists(r.ctx, metadataKey).Result()
			if err != nil {
				return err
			}
			if exists == 0 {
				return errors.New("metadata key does not exist")
			}

			groupsStr, err := tx.HGet(r.ctx, metadataKey, "groups").Result()
			if err != nil {
				return err
			}

			//This works if groupsToAdd and groupsToRemove are disjointed
			if groupsStr == "" {
				groups = groupsToAdd
			} else {
				groups = strings.Split(groupsStr, ",")
				for _, g := range groupsToAdd {
					if !slices.Contains(groups, g) {
						groups = append(groups, g)
					}
				}
				groups = slices.DeleteFunc(groups, func(g string) bool {
					return slices.Contains(groupsToRemove, g)
				})
			}

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.HSet(r.ctx, metadataKey, "groups", strings.Join(groups, ","))
				seqCmd = pipe.HIncrBy(r.ctx, metadataKey, "groupsSequence", int64(1))

				return nil
			})
			if err != nil {
				return err
			}

			seq, err = seqCmd.Uint64()
			if err != nil {
				return err
			}

			return err
		}, metadataKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("UpdateGroupsInMetadata: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("UpdateGroupsInMetadata: Failed to update groups to metadata", "collection_id", collectionId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "error", err)
			return nil, 0, err
		}

		r.log.Info("UpdateGroupsInMetadata: Metadata groups updated successfully", "collection_id", collectionId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
		r.log.Debug("AddGroupsToMetadata: Metadata groups updated successfully", "new_groups", groups, "new_sequence", seq)
		return groups, seq, nil
	}

	r.log.Warn("UpdateGroupsInMetadata failed due to retries exhaustion. The groups has NOT been updated in metadata", "collection_id", collectionId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
	return nil, 0, errors.New("transaction retries exhausted")
}

func (r *RedisClient) SetGroupsInMetadata(collectionId string, sequenceGroups uint64, groups ...string) error {
	r.log.Info("SetGroupsInMetadata called", "collection_id", collectionId, "sequence_groups", sequenceGroups, "groups", groups)

	metadataKey := "meta:" + collectionId

	backoff := defaultBackoff

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {

			exists, err := tx.Exists(r.ctx, metadataKey).Result()
			if err != nil {
				return err
			}
			if exists == 0 {
				return errors.New("metadata key does not exist")
			}

			groupsSeq, err := tx.HGet(r.ctx, metadataKey, "groupsSequence").Uint64()
			if err != nil {
				return err
			}

			if groupsSeq >= sequenceGroups {
				return nil
			}

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.HMSet(r.ctx, metadataKey, "groups", strings.Join(groups, ","), "groupsSequence", sequenceGroups)

				return nil
			})
			if err != nil {
				return err
			}

			return err
		}, metadataKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("SetGroupsInMetadata: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "sequenceGroups", sequenceGroups, "groups", groups)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("SetGroupsInMetadata: Failed to set groups in metadata", "collection_id", collectionId, "groups_sequence", sequenceGroups, "groups", groups, "error", err)
			return err
		}

		r.log.Info("SetGroupsInMetadata: Metadata groups set successfully", "collection_id", collectionId, "groups_sequence", sequenceGroups, "groups", groups)
		return nil
	}

	r.log.Warn("SetGroupsInMetadata failed due to retries exhaustion. The groups have NOT been set in metadata", "collection_id", collectionId, "sequence_groups", sequenceGroups, "groups", groups)
	return errors.New("max retries exceeded")
}

func (r *RedisClient) IsAuthorizedInMetadata(collectionId string, userId string, userGroups ...string) (bool, error) {
	r.log.Info("IsAuthorizedInMetadata called", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)

	if len(userGroups) == 0 {
		return false, nil
	}

	collMetadataAuthorizations, err := r.client.HMGet(r.ctx, "meta:"+collectionId, "owner", "groups").Result()
	if err != nil {
		r.log.Error("IsAuthorizedInMetadata: Failed to check user authorization searching in metadata", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups, "error", err)
		return false, err
	}

	collOwner := collMetadataAuthorizations[0].(string)
	collGroupsStr := collMetadataAuthorizations[1].(string)

	if collOwner == userId {
		r.log.Info("IsAuthorizedInMetadata: Authorization check successful. User is the collection's owner", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)
		r.log.Debug("IsAuthorizedInMetadata: Authorization check successful. User is the collection's owner", "collection_owner", collOwner, "collection_groups", collGroupsStr)
		return true, nil
	}

	if collGroupsStr == "" {
		r.log.Warn("IsAuthorizedInMetadata: Failed authorization check. User is not collection's owner and the collection does not have any group associated", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)
		r.log.Debug("IsAuthorizedInMetadata: Failed authorization check. User is not collection's owner and the collection does not have any group associated", "collection_owner", collOwner, "collection_groups", collGroupsStr)
		return false, nil
	}

	collGroups := strings.Split(collGroupsStr, ",")

	if slices.Contains(collGroups, model.PublicGroup) {
		r.log.Info("IsAuthorizationInMetadata: Authorization check successful. The collection is marked as public", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)
		r.log.Debug("IsAuthorizationInMetadata: Authorization check successful. The collection is marked as public", "collection_owner", collOwner, "collection_groups", collGroups)
		return true, nil
	}

	for _, group := range userGroups {
		if slices.Contains(collGroups, group) {
			r.log.Info("IsAuthorizationInMetadata: Authorization check successful. The user is in a group associated to the collection", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)
			r.log.Debug("IsAuthorizationInMetadata: Authorization check successful. The user is in a group associated to the collection", "collection_owner", collOwner, "collection_groups", collGroups)
			return true, nil
		}
	}

	r.log.Warn("IsAuthorizedInMetadata: Authorization failed. The user has no rights to access the collection", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups, "collection_owner", collOwner, "collection_groups", collGroups)
	return false, nil
}

func (r *RedisClient) SetGroups(collectionId string, sequenceGroups uint64, groups ...string) error {
	r.log.Info("SetGroups called", "collection_id", collectionId, "sequence_groups", sequenceGroups, "groups", groups)

	groupsKey := "info:" + collectionId + ":groups"
	groupsSequenceKey := "info:" + collectionId + ":groupsSequence"
	infoKey := "info:" + collectionId

	backoff := defaultBackoff

	var toAdd []any
	for _, member := range groups {
		toAdd = append(toAdd, member)
	}

	for retries := 0; retries < maxRetries; retries++ {
		err := r.client.Watch(r.ctx, func(tx *redis.Tx) error {
			exists, err := tx.Exists(r.ctx, infoKey).Result()
			if err != nil {
				return err
			}
			if exists == 0 {
				return errors.New("groups key does not exist")
			}

			seq, err := tx.Get(r.ctx, groupsSequenceKey).Uint64()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if seq >= sequenceGroups {
				return nil
			}

			_, err = tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(r.ctx, groupsKey)
				pipe.SAdd(r.ctx, groupsKey, toAdd...)
				pipe.Set(r.ctx, groupsSequenceKey, sequenceGroups, 0)

				return nil
			})
			return err
		}, infoKey, groupsKey, groupsSequenceKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("SetGroups: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "sequence_groups", sequenceGroups, "groups", groups)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if err != nil {
			r.log.Error("SetGroups: Failed to set groups", "collection_id", collectionId, "groups_sequence", sequenceGroups, "groups", groups, "error", err)
			return err
		}

		r.log.Info("SetGroups: Groups set successfully", "collection_id", collectionId, "groups_sequence", sequenceGroups, "groups", groups)
		return nil
	}

	r.log.Warn("SetGroups failed due to retries exhaustion. The groups has NOT been set", "collection_id", collectionId, "sequence_groups", sequenceGroups, "groups", groups)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) IsAuthorized(collectionId string, userId string, userGroups ...string) (bool, error) {
	r.log.Info("IsAuthorized called", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)

	ownerKey := "info:" + collectionId + ":owner"

	m := make([]any, len(userGroups))
	for i, member := range userGroups {
		m[i] = member
	}

	storedOwner, err := r.client.Get(r.ctx, ownerKey).Result()
	if storedOwner == userId {
		r.log.Info("IsAuthorized: Authorization check successful. User is the collection's owner", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)
		r.log.Debug("IsAuthorized: Authorization check successful. User is the collection's owner", "collection_owner", storedOwner)
		return true, nil
	}

	res, err := r.client.SMIsMember(r.ctx, "info:"+collectionId+":groups", append(m, model.PublicGroup)...).Result()
	if err != nil {
		r.log.Error("IsAuthorized: Authorization check failed", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups, "error", err)
		return false, err
	}

	r.log.Info("IsAuthorized: Authorization check successful", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups, "collection_owner", storedOwner, "can_access", slices.Contains(res, true))
	r.log.Debug("IsAuthorized: Authorization check successful", "can_access", slices.Contains(res, true))
	return slices.Contains(res, true), err
}

func (r *RedisClient) CreateIndex(customMetadataKeys ...string) error {
	r.log.Info("CreateIndex called", "custom_metadata_keys", customMetadataKeys)

	fields := make([]*redis.FieldSchema, len(customMetadataKeys))
	for i, key := range customMetadataKeys {
		fields[i] = &redis.FieldSchema{
			FieldName: key,
			FieldType: redis.SearchFieldTypeTag,
		}
	}

	err := r.client.FTCreate(r.ctx, "metadata_index", &redis.FTCreateOptions{
		OnHash: true,
		Prefix: []any{1, "meta:"},
	}, append([]*redis.FieldSchema{{
		FieldName: "name",
		FieldType: redis.SearchFieldTypeTag,
	}, {
		FieldName: "owner",
		FieldType: redis.SearchFieldTypeTag,
	}, {
		FieldName: "created_at",
		FieldType: redis.SearchFieldTypeNumeric,
	}, {
		FieldName: "groups",
		FieldType: redis.SearchFieldTypeTag,
	}}, fields...)...).Err()

	if err != nil {
		r.log.Error("CreateIndex: Failed to create index", "custom_metadata_keys", customMetadataKeys, "error", err)
		return err
	}

	r.log.Info("CreateIndex: Index created successfully")
	return nil
}

func (r *RedisClient) SearchOnMetadata(query model.QueryTemplate, userId string, userGroups []string) ([]types.CollectionInfo, error) {
	r.log.Info("SearchOnMetadata called", "query", fmt.Sprintf("%+v", query), "user_id", userId, "user_access_groups", userGroups)

	redisQuery, err := query.ToRedisSearchCommand()
	if err != nil {
		return nil, err
	}

	var authorizationPart string
	if userId != "" {
		if len(userGroups) > 0 {
			authorizationPart = " ((@groups:{" + strings.Join(userGroups, " | ") + "}) | (@owner:{" + userId + "}))"
		} else {
			authorizationPart = " @owner:{" + userId + "}"
		}
	} else {
		if len(userGroups) > 0 {
			authorizationPart = " @groups:{" + strings.Join(userGroups, " | ") + "}"
		}
	}

	redisQuery = redisQuery + authorizationPart

	res, err := r.client.FTSearch(r.ctx, "metadata_index", redisQuery).Result()
	if err != nil {
		r.log.Error("SearchOnMetadata: Failed to search index", "query", fmt.Sprintf("%+v", query), "user_id", userId, "user_access_groups", userGroups, "query_redis_format", redisQuery, "error", err)
		return nil, err
	}

	var result []types.CollectionInfo
	for _, doc := range res.Docs {
		collId, _ := strings.CutPrefix(doc.ID, "meta:")

		delete(doc.Fields, "groupsSequence")
		metadata, err := model.FromMap(doc.Fields)
		if err != nil {
			r.log.Error("SearchOnMetadata: Failed to parse metadata", "query", fmt.Sprintf("%+v", query), "user_id", userId, "user_access_groups", userGroups, "query_redis_format", redisQuery, "redis_response", fmt.Sprintf("%+v", doc), "error", err)
			return nil, err
		}

		collectionInfo := types.CollectionInfo{
			Id:       collId,
			Metadata: *metadata,
		}

		result = append(result, collectionInfo)
	}

	r.log.Info("SearchOnMetadata: Query executed successfully", "query", fmt.Sprintf("%+v", query), "user_id", userId, "user_access_groups", userGroups)
	r.log.Info("SearchOnMetadata: Query executed successfully", "query_result", fmt.Sprintf("%+v", result))
	return result, nil
}

func (r *RedisClient) DropIndex() error {
	r.log.Info("DropIndex called")

	err := r.client.FTDropIndex(r.ctx, "metadata_index").Err()
	if err != nil {
		r.log.Error("DropIndex: Failed to drop index", "error", err)
		return err
	}

	r.log.Info("DropIndex: Index dropped successfully")
	return nil
}

func (r *RedisClient) AlterIndex(customMetadataKeys ...string) error {
	r.log.Info("AlterIndex called", "custom_metadata_keys", customMetadataKeys)

	fields := make([]any, len(customMetadataKeys)*2)
	for i := 0; i < len(fields); i += 2 {
		fields[i] = customMetadataKeys[i/2]
		fields[i+1] = "TAG"
	}

	err := r.client.FTAlter(r.ctx, "metadata_index", true, fields).Err()
	if err != nil {
		r.log.Error("AlterIndex: Failed to alter index", "custom_metadata_keys", customMetadataKeys, "error", err)
		return err
	}

	r.log.Info("AlterIndex: Index altered successfully", "custom_metadata_keys", customMetadataKeys)
	return nil
}

func (r *RedisClient) GetMaxMemory() (uint64, error) {
	r.log.Info("GetMaxMemory called")

	stats, err := r.client.ConfigGet(r.ctx, "maxmemory").Result()
	if err != nil {
		r.log.Error("GetMaxMemory: Failed to get maxmemory config", "error", err)
		return 0, err
	}
	if val, ok := stats["maxmemory"]; ok {
		r.log.Info("GetMaxMemory: Maxmemory retrieved successfully")
		r.log.Debug("GetMaxMemory: Maxmemory retrieved successfully", "maxmemory_value", val)
		return strToInt(val), nil
	}

	r.log.Error("GetMaxMemory: Failed to get maxmemory config", "error", "couldn't find the 'maxmemory' key in the config map")
	return 0, fmt.Errorf("failed to get maxmemory config")
}

func (r *RedisClient) GetAvailableMemory() (uint64, error) {
	r.log.Info("GetAvailableMemory called")

	maxMem, err := r.GetMaxMemory()
	if err != nil {
		r.log.Error("GetAvailableMemory: Failed to get maxmemory", "error", err)
		return 0, err
	}
	if maxMem == 0 {
		r.log.Warn("GetAvailableMemory: Maxmemory is unlimited")
		return ^uint64(0), nil
	}

	stats, err := r.GetMemoryStats()
	if err != nil {
		r.log.Error("GetAvailableMemory: Failed to get current memory statistics", "error", err)
		return 0, err
	}

	r.log.Info("GetAvailableMemory: Available memory retrieved successfully")
	r.log.Debug("GetAvailableMemory: Available memory retrieved successfully", "available_memory", maxMem-stats.CurrentUsedMemory)
	return maxMem - stats.CurrentUsedMemory, nil
}

func (r *RedisClient) GetMemoryStats() (*db.MemStats, error) {
	r.log.Info("GetMemoryStats called")

	stats, err := r.client.Info(r.ctx, "MEMORY", "KEYSPACE").Result()
	if err != nil {
		r.log.Error("GetMemoryStats: Failed to get current memory statistics", "error", err)
		return nil, err
	}

	statsMap := parseRedisInfo(stats)

	var keysCount, bytesKeysRatio uint64 = 0, 0

	re := regexp.MustCompile(`keys=(\d+)`)
	match := re.FindStringSubmatch(statsMap["db0"])
	if len(match) == 2 {
		keysCount = strToInt(match[1])
		bytesKeysRatio = strToInt(statsMap["used_memory_dataset"]) / keysCount
	}

	datasetPerc, err := strconv.ParseFloat(statsMap["used_memory_dataset_perc"][:len(statsMap["used_memory_dataset_perc"])-1], 64)
	if err != nil {
		r.log.Error("GetMemoryStats: Failed to parse dataset perc", "error", err)
		return nil, err
	}

	memStats := db.MemStats{
		AllocationStats: db.AllocationStats{
			UsedMemoryPeak:              strToInt(statsMap["used_memory_peak"]),
			CurrentUsedMemory:           strToInt(statsMap["used_memory"]),
			UsedMemoryOverhead:          strToInt(statsMap["used_memory_overhead"]),
			UsedMemoryStartup:           strToInt(statsMap["used_memory_startup"]),
			UsedMemoryDataset:           strToInt(statsMap["used_memory_dataset"]),
			UsedMemoryDatasetPercentage: datasetPerc / 100,
		},
		ReplicationBacklogSize: strToInt(statsMap["mem_replication_backlog"]),
		LuaScriptsCacheSize:    strToInt(statsMap["used_memory_scripts"]),
		KeysCount:              keysCount,
		BytesKeysRatio:         bytesKeysRatio,
	}

	r.log.Info("GetMemoryStats: Memory stats retrieved successfully")
	r.log.Debug("GetMemoryStats: Memory stats retrieved successfully", "mem_stats", fmt.Sprintf("%+v", memStats))
	return &memStats, err
}

func (r *RedisClient) GetCollectionMemoryUsage(collectionId string) (uint64, error) {
	r.log.Info("GetCollectionMemoryUsage called", "collection_id", collectionId)

	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"
	groupSequenceKey := "info:" + collectionId + ":groupsSequence"
	infoKey := "info:" + collectionId
	ownerKey := "info:" + collectionId + ":owner"

	var totalMemory uint64

	// We do not use a transaction here because we don't care about getting a precise value,
	// as the memory usage may change while we are calculating it.
	// We just want to get an approximate value of the memory usage of the collection.
	keys, err := r.client.SMembers(r.ctx, keysKey).Result()
	if err != nil {
		r.log.Error("GetCollectionMemoryUsage: Failed to retrieve collection memory usage", "collection_id", collectionId, "error", err)
		return 0, err
	}

	totalMemory = 0

	for _, key := range keys {
		memUsageValue, err := r.client.MemoryUsage(r.ctx, buildKey(collectionId, key)).Result()
		if err != nil {
			r.log.Error("GetCollectionMemoryUsage: Failed to retrieve collection memory usage", "collection_id", collectionId, "error", err)
			return 0, err
		}
		memUsageTimestamp, err := r.client.MemoryUsage(r.ctx, buildTimestampKey(collectionId, key)).Result()
		if err != nil {
			r.log.Error("GetCollectionMemoryUsage: Failed to retrieve collection memory usage", "collection_id", collectionId, "error", err)
			return 0, err
		}
		totalMemory += uint64(memUsageValue + memUsageTimestamp)
	}

	metaKeys := []string{keysKey, groupsKey, infoKey, groupSequenceKey, ownerKey}
	for _, metaKey := range metaKeys {
		memUsage, err := r.client.MemoryUsage(r.ctx, metaKey).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.log.Error("GetCollectionMemoryUsage: Failed to retrieve collection memory usage", "collection_id", collectionId, "error", err)
			return 0, err
		}
		totalMemory += uint64(memUsage)
	}

	r.log.Info("GetCollectionMemoryUsage: Collection memory usage retrieved successfully", "collection_id", collectionId)
	r.log.Debug("GetCollectionMemoryUsage: Collection memory usage retrieved successfully", "collection_id", collectionId, "collection_memory_usage", totalMemory)
	return totalMemory, nil
}

func (r *RedisClient) GetRecordMemoryUsage(collectionId string, recordName string) (uint64, error) {
	r.log.Info("GetRecordMemoryUsage called", "collection_id", collectionId, "record_name", recordName)

	// We do not use a transaction here because we don't care about getting a precise value,
	// as the memory usage may change while we are calculating it.
	// We just want to get an approximate value of the memory usage of the collection.
	memUsageValue, err := r.client.MemoryUsage(r.ctx, buildKey(collectionId, recordName)).Result()
	if err != nil {
		r.log.Error("GetRecordMemoryUsage: Failed to retrieve record memory usage", "collection_id", collectionId, "record_name", recordName, "error", err)
		return 0, err
	}

	memUsageTimestamp, err := r.client.MemoryUsage(r.ctx, buildTimestampKey(collectionId, recordName)).Result()
	if err != nil {
		r.log.Error("GetRecordMemoryUsage: Failed to retrieve timestamp memory usage", "collection_id", collectionId, "record_name", recordName, "error", err)
		return 0, err
	}

	r.log.Info("GetRecordMemoryUsage: Record memory usage retrieved successfully", "collection_id", collectionId, "record_name", recordName)
	r.log.Debug("GetRecordMemoryUsage: Record memory usage retrieved successfully", "collection_id", collectionId, "record_name", recordName, "record_memory_usage", memUsageValue, "timestamp_memory_usage", memUsageTimestamp)
	return uint64(memUsageValue + memUsageTimestamp), err
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}

func buildKey(collectionId string, recordName string) string {
	return "data:" + collectionId + ":" + recordName
}
func buildTimestampKey(collectionId string, recordName string) string {
	return "info:" + collectionId + ":" + recordName + ":timestamp"
}

func buildKeys(collectionId string, recordNames []string) []string {
	keys := slices.Clone(recordNames)
	for i, name := range keys {
		keys[i] = buildKey(collectionId, name)
	}
	return keys
}

func buildTimestampKeys(collectionId string, recordNames []string) []string {
	keys := slices.Clone(recordNames)
	for i, name := range keys {
		keys[i] = buildTimestampKey(collectionId, name)
	}
	return keys
}

func buildNamesAndKeys(collectionId string, recordNames []string) ([]any, []string) {
	keys := slices.Clone(recordNames)
	names := make([]any, len(recordNames))
	for i, name := range keys {
		keys[i] = buildKey(collectionId, name)
		names[i] = name
	}
	return names, keys
}

func toListAny(s []string) []any {
	res := make([]any, len(s))
	for i, v := range s {
		res[i] = v
	}
	return res
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
