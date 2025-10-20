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
	return r.client.Get(r.ctx, buildKey(collectionId, recordName)).Result()
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

		return err
	}

	r.log.Warn("SetRecord failed due to retries exhaustion. The record has NOT been set", "collection_id", collectionId, "record_name", recordName, "value", value, "timestamp", timestamp)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	r.log.Info("GetRecords called", "collection_id", collectionId, "record_names", recordNames)
	return r.client.MGet(r.ctx, buildKeys(collectionId, recordNames)...).Result()
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

		return err
	}

	r.log.Warn("SetRecords failed due to retries exhaustion. The records have NOT been set", "collection_id", collectionId, "record_names", recordNames, "record_values", recordValues, "timestamp", timestamp)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) RecordExists(collectionId string, recordName string) (bool, error) {
	r.log.Info("RecordExists called", "collection_id", collectionId, "record_name", recordName)

	res, err := r.client.Exists(r.ctx, buildKey(collectionId, recordName)).Result()
	return res > 0, err
}

func (r *RedisClient) DeleteRecords(collectionId string, timestamp string, recordNames ...string) error {
	r.log.Info("DeleteRecords called", "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp)

	keysKey := "info:" + collectionId + ":keys"
	infoKey := "info:" + collectionId

	backoff := defaultBackoff

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

		return err
	}

	r.log.Warn("DeleteRecords failed due to retries exhaustion. The records have NOT been deleted", "collection_id", collectionId, "record_names", recordNames, "timestamp", timestamp)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) GetCollection(collectionId string) (map[string]string, []string, string, uint64, error) {
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

			cmds, err := tx.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
				pipe.MGet(r.ctx, buildKeys(collectionId, keys)...)
				return nil
			})
			if err != nil {
				return err
			}

			mgetCmd := cmds[0].(*redis.SliceCmd)
			values, err = mgetCmd.Result()
			if err != nil {
				return err
			}

			return nil
		}, keysKey, groupsKey, groupsSequenceKey)

		if err == nil {
			result := make(map[string]string, len(keys))
			for i, key := range keys {
				result[key] = values[i].(string)
			}

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

		return nil, nil, "", 0, err
	}

	r.log.Warn("GetCollection failed due to retries exhaustion. The collection has NOT been returned", "collection_id", collectionId)
	return nil, nil, "", 0, errors.New("transaction retries exhausted")
}

// Implemented like this, we allow the overwrite of a collection. If we want to avoid to overwrite a collection
// we would need to add a Watch( ..., infoKey) to check the existence of the collection
func (r *RedisClient) SetCollection(collectionId string, records map[string]string, groups []string, owner string, groupsSequence uint64) error {
	r.log.Info("SetCollection called", "collection_id", collectionId, "recors", fmt.Sprintf("%+v", records), "groups", groups, "owner", owner, "groupsSequence", groupsSequence)

	keysKey := "info:" + collectionId + ":keys"
	groupsKey := "info:" + collectionId + ":groups"
	groupsSequenceKey := "info:" + collectionId + ":groupsSequence"
	ownerKey := "info:" + collectionId + ":owner"

	groupsAny := toListAny(groups)

	_, err := r.client.TxPipelined(r.ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(r.ctx, "info:"+collectionId, "", 0)
		pipe.Set(r.ctx, ownerKey, owner, 0)

		for key, value := range records {
			pipe.Set(r.ctx, buildKey(collectionId, key), value, 0)
			pipe.SAdd(r.ctx, keysKey, key)
		}

		if len(groupsAny) > 0 {
			pipe.SAdd(r.ctx, groupsKey, groupsAny...)
		}
		pipe.Set(r.ctx, groupsSequenceKey, groupsSequence, 0)

		return nil
	})

	return err
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
			allKeysToDelete := append(dataKeys, infoKey, keysKey, groupsKey, groupsSequenceKey, ownerKey)

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

		return err
	}

	r.log.Warn("DeleteCollection failed due to retries exhaustion. The collection has NOT been deleted", "collection_id", collectionId)
	return errors.New("transaction retries exhausted")
}

func (r *RedisClient) CollectionExists(collectionId string) (bool, error) {
	r.log.Info("CollectionExists called", "collection_id", collectionId)

	res, err := r.client.Exists(r.ctx, "info:"+collectionId).Result()
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func (r *RedisClient) GetMetadata(collectionId string) (*model.Metadata, error) {
	r.log.Info("GetMetadata called", "collection_id", collectionId)

	res, err := r.client.HGetAll(r.ctx, "meta:"+collectionId).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	delete(res, "groupsSequence")
	return model.FromMap(res)
}

func (r *RedisClient) SetMetadata(collectionId string, metadata model.Metadata) error {
	r.log.Info("SetMetadata called", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", metadata))

	metaList := append(metadata.ToList(), "groupsSequence", 0)
	return r.client.HSet(r.ctx, "meta:"+collectionId, metaList).Err()
}

func (r *RedisClient) MetadataExists(collectionId string) (bool, error) {
	r.log.Info("MetadataExists called", "collection_id", collectionId)

	res, err := r.client.Exists(r.ctx, "meta:"+collectionId).Result()
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
			return false, err
		}

		return !exists, nil
	}

	r.log.Warn("IsUniqueForOwner failed due to retries exhaustion. The pair (<collection_name>,<owner>) has NOT been 'declared' as existing. New calls may 'declare' it", "collection_name", collectionName, "owner", owner)
	return false, errors.New("transaction retries exhausted")
}

func (r *RedisClient) DeleteMetadata(collectionId string) error {
	r.log.Info("DeleteMetadata called", "collection_id", collectionId)

	return r.client.Del(r.ctx, "meta:"+collectionId).Err()
}

// TODO: Probably not needed anymore
func (r *RedisClient) AddGroupsToMetadata(collectionId string, groupsToAdd ...string) ([]string, uint64, error) {
	r.log.Info("AddGroupsToMetadata called", "collection_id", collectionId, "groups_to_add", groupsToAdd)

	if len(groupsToAdd) == 0 {
		return nil, 0, nil
	}

	metadataKey := "meta:" + collectionId

	backoff := defaultBackoff

	var seq uint64
	var groups []string

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
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			if groupsStr == "" {
				groups = groupsToAdd
			} else {
				groups = strings.Split(groupsStr, ",")
				for _, g := range groupsToAdd {
					if !slices.Contains(groups, g) {
						groups = append(groups, g)
					}
				}
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

			return nil
		}, metadataKey)

		if errors.Is(err, redis.TxFailedErr) {
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			r.log.DebugfWith("AddGroupsToMetadata: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "groups_to_add", groupsToAdd)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		return groups, seq, err
	}

	r.log.Warn("AddGroupsToMetadata failed due to retries exhaustion. The groups has NOT been added to metadata", "collection_id", collectionId, "groups_to_add", groupsToAdd)
	return nil, 0, errors.New("transaction retries exhausted")
}

// TODO: Probably not needed anymore
func (r *RedisClient) RemoveGroupsFromMetadata(collectionId string, groupsToRemove ...string) ([]string, uint64, error) {
	r.log.Info("RemoveGroupsFromMetadata called", "collection_id", collectionId, "groups_to_remove", groupsToRemove)

	if len(groupsToRemove) == 0 {
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

			groups = strings.Split(groupsStr, ",")
			groups = slices.DeleteFunc(groups, func(g string) bool {
				return slices.Contains(groupsToRemove, g)
			})

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
			r.log.DebugfWith("RemoveGroupsFromMetadata: Transaction failed due to high concurrency. Retrying with %s delay", []any{backoff + jitter}, "collection_id", collectionId, "groups_to_remove", groupsToRemove)
			time.Sleep(backoff + jitter)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		return groups, seq, err

	}

	r.log.Warn("RemoveGroupsFromMetadata failed due to retries exhaustion. The groups has NOT been removed from metadata", "collection_id", collectionId, "groups_to_remove", groupsToRemove)
	return nil, 0, errors.New("transaction retries exhausted")
}

func (r *RedisClient) UpdateGroupsInMetadata(collectionId string, groupsToAdd []string, groupsToRemove []string) ([]string, uint64, error) {
	r.log.Info("UpdateGroupsInMetadata called", "collection_id", collectionId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)

	if len(groupsToRemove) == 0 {
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

		return groups, seq, err
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

		return err
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
		return false, err
	}

	collOwner := collMetadataAuthorizations[0].(string)
	collGroupsStr := collMetadataAuthorizations[1].(string)

	if collOwner == userId {
		return true, nil
	}

	if collGroupsStr == "" {
		return false, nil
	}

	collGroups := strings.Split(collGroupsStr, ",")

	if slices.Contains(collGroups, model.PublicGroup) {
		return true, nil
	}

	for _, group := range userGroups {
		if slices.Contains(collGroups, group) {
			return true, nil
		}
	}

	r.log.Warn("IsAuthorizedInMetadata failed due to retries exhaustion. The user has been marked as UNAUTHORIZED, despite potentially having the necessary right accesses", "collection_id", collectionId, "user_id", userId, "user_access_groups", userGroups)
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
		return err
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
		return true, nil
	}

	res, err := r.client.SMIsMember(r.ctx, "info:"+collectionId+":groups", append(m, model.PublicGroup)...).Result()
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

	return r.client.FTCreate(r.ctx, "metadata_index", &redis.FTCreateOptions{
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
			authorizationPart = " ((@groups:{" + strings.Join(userGroups, " | ") + "}) | (@userId:{" + userId + "}))"
		} else {
			authorizationPart = " @userId:{" + userId + "}"
		}
	} else {
		if len(userGroups) > 0 {
			authorizationPart = " @groups:{" + strings.Join(userGroups, " | ") + "}"
		}
	}

	redisQuery = redisQuery + authorizationPart

	res, err := r.client.FTSearch(r.ctx, "metadata_index", redisQuery).Result()
	if err != nil {
		return nil, err
	}

	var result []types.CollectionInfo
	for _, doc := range res.Docs {
		collId, _ := strings.CutPrefix(doc.ID, "meta:")

		delete(doc.Fields, "groupsSequence")
		metadata, err := model.FromMap(doc.Fields)
		if err != nil {
			return nil, err
		}

		collectionInfo := types.CollectionInfo{
			Id:       collId,
			Metadata: *metadata,
		}

		result = append(result, collectionInfo)
	}

	return result, nil
}

func (r *RedisClient) DropIndex() error {
	r.log.Info("DropIndex called")

	return r.client.FTDropIndex(r.ctx, "metadata_index").Err()
}

func (r *RedisClient) AlterIndex(customMetadataKeys ...string) error {
	r.log.Info("AlterIndex called", "custom_metadata_keys", customMetadataKeys)

	fields := make([]any, len(customMetadataKeys)*2)
	for i := 0; i < len(fields); i += 2 {
		fields[i] = customMetadataKeys[i/2]
		fields[i+1] = "TAG"
	}

	return r.client.FTAlter(r.ctx, "metadata_index", true, fields).Err()
}

func (r *RedisClient) GetMaxMemory() (uint64, error) {
	r.log.Info("GetMaxMemory called")

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
	r.log.Info("GetAvailableMemory called")

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
	r.log.Info("RecordExists called")

	stats, err := r.client.Info(r.ctx, "MEMORY", "KEYSPACE").Result()
	if err != nil {
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
		KeysCount:              keysCount,
		BytesKeysRatio:         bytesKeysRatio,
	}

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

	// TODO: Fix this. Watch without TxPipelined does not make sense
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

		metaKeys := []string{keysKey, groupsKey, infoKey, groupSequenceKey, ownerKey}
		for _, metaKey := range metaKeys {
			memUsage, err := tx.MemoryUsage(r.ctx, metaKey).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			totalMemory += uint64(memUsage)
		}

		return nil
	}, keysKey, infoKey)

	if err != nil {
		return 0, err
	}

	return totalMemory, nil
}

func (r *RedisClient) GetRecordMemoryUsage(collectionId string, recordName string) (uint64, error) {
	r.log.Info("GetRecordMemoryUsage called", "collection_id", collectionId, "record_name", recordName)

	memUsage, err := r.client.MemoryUsage(r.ctx, buildKey(collectionId, recordName)).Result()
	return uint64(memUsage), err
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
