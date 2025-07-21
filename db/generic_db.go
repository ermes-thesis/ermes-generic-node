package db

import (
	"config"
	"context"
	"github.com/redis/go-redis/v9"
	"metadataManager/utils"
)

type GenericDB interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	MGet(keys ...string) ([]any, error)
	Exists(key string) (bool, error)
	Del(key ...string) error
	SetMetadata(key string, metadata Metadata) error
	GetMetadata(key string, field string) (string, error)
	GetAllMetadata(key string) (*Metadata, error)
	AddGroup(key string, members ...string) error
	IsInGroups(key string, members ...string) (bool, error)
	RemoveGroup(key string, members ...string) error
	CreateIndex(customMetadataKeys ...string) error
	SearchOnMetadata(query utils.Query, groups []string) ([]CollectionInfo, error)
	DropIndex() error
	AlterIndex(customMetadataKeys ...string) error
	Close() error
}

func NewRedisClient(config config.RedisConfig, ctx context.Context) GenericDB {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.Address + ":" + config.Port,
		Password: config.Password,
		DB:       config.DB,
		Protocol: config.Protocol,
	})
	return &RedisClient{client: rdb, ctx: ctx}
}
