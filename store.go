package store

import (
	"context"
	"errors"
	"time"
)

var ErrProviderNotFound = errors.New("STORE_PROVIDER_NOT_FOUND")

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

type Store interface {
	Get(ctx context.Context, key string) (response []byte, meta *TsMeta, err error)
	GetKeyValueByTimestamp(ctx context.Context, pattern, fromTimestamp, toTimestamp string) chan *GetKeyValueByTimestampC
	GetByQuery(ctx context.Context, pattern, query, rest string) (*QueryPaginatedResponse, error)
	Set(ctx context.Context, key string, value []byte) (err error)
	SetWithTTL(ctx context.Context, key string, value []byte, duration time.Duration) (err error)
	Delete(ctx context.Context, key string) (err error)
	CloseConn() (err error)
}

func InitializeStore(provider string, meta PgMeta) (Store, error) {
	switch provider {
	// case "redis":
	// 	store, err := NewRedisStore(meta)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return store, nil

	case "pgsql":
		store, err := NewPostgresStore(meta)
		if err != nil {
			return nil, err
		}
		return store, nil
	}

	return nil, ErrProviderNotFound
}
