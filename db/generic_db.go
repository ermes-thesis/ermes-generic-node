package db

import (
	"config"
	"context"
	"github.com/redis/go-redis/v9"
	"metadataManager/utils"
)

type GenericDB interface {
	GetRecord(collectionId string, recordName string) (string, error)
	SetRecord(collectionId string, recordName string, value string) error
	GetRecords(collectionId string, recordNames ...string) ([]any, error)
	RecordExists(collectionId string, recordName string) (bool, error)
	DeleteRecords(collectionId string, recordNames ...string) error

	GetCollection(collectionId string) (map[string]string, []string, error)
	SetCollection(collectionId string, records map[string]string, groups []string) error
	DeleteCollection(collectionId string) error
	CollectionExists(collectionId string) (bool, error)

	SetMetadata(collectionId string, metadata Metadata) error
	GetMetadata(collectionId string) (*Metadata, error)
	DeleteMetadata(collectionId string) error

	AddGroups(collectionId string, members ...string) error
	IsInGroups(collectionId string, members ...string) (bool, error)
	RemoveGroups(collectionId string, members ...string) error

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
