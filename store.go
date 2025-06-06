package store

import (
	"context"
	"errors"
	"time"
)

var ErrProviderNotFound = errors.New("STORE_PROVIDER_NOT_FOUND")
var ErrKeyNotFound = errors.New("KEY_NOT_FOUND")
var ErrKeyAlreadyExists = errors.New("KEY_ALREADY_EXISTS")

type QueryResponse struct {
	Data      []byte
	ExpiresAt string
	CreatedAt string
}

type QueryPaginatedResponse struct {
	QueryResponse []QueryResponse
	TotalRecords  int
}

type TsMeta struct {
	CreatedAt string
	ExpiresAt string
}

type KeyValuePair struct {
	Key   []byte
	Value []byte
}

type GetKeyValueByTimestampC struct {
	Response *KeyValuePair
	Err      error
}

type StoreType any

type Store interface {
	Set(ctx context.Context, key string, value []byte) (err error)
	SetWithTTL(ctx context.Context, key string, value []byte, duration time.Duration) (err error)
	Delete(ctx context.Context, key string) (err error)
	CloseConn() (err error)
}

func InitializeStore(provider string, meta StoreType) (Store, error) {
	switch provider {
	case "redis":
		store, err := NewRedisStore(meta.(RedisMeta))
		if err != nil {
			return nil, err
		}
		return store, nil

	case "pgsql":
		store, err := NewPostgresStore(meta.(PgMeta))
		if err != nil {
			return nil, err
		}
		return store, nil
	}

	return nil, ErrProviderNotFound
}
