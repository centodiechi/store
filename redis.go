package store

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
	meta   RedisMeta
}

type RedisMeta struct {
	Host     string `json:"host" yaml:"host"`
	Port     string `json:"port" yaml:"port"`
	Password string `json:"password" yaml:"password"`
	DB       int    `json:"db" yaml:"db"`
}

func NewRedisStore(meta RedisMeta) (Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", meta.Host, meta.Port),
		Password: meta.Password,
		DB:       meta.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisStore{
		client: client,
		meta:   meta,
	}, nil
}

func (r *RedisStore) GetNextID(ctx context.Context) (int64, error) {
	key := "generator/id"
	id, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to generate ID: %w", err)
	}
	return id, nil
}

func (r *RedisStore) Get(ctx context.Context, key string) ([]byte, *TsMeta, error) {
	fullKey := key
	val, err := r.client.Get(ctx, fullKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, nil, err
	}

	ttl, err := r.client.TTL(ctx, fullKey).Result()
	if err != nil {
		return nil, nil, err
	}

	meta := &TsMeta{
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	if ttl > 0 {
		meta.ExpiresAt = time.Now().Add(ttl).Format(time.RFC3339)
	}

	return val, meta, nil
}

func (r *RedisStore) Set(ctx context.Context, key string, value []byte) error {
	return r.client.Set(ctx, key, value, 0).Err()
}

func (r *RedisStore) SetWithTTL(ctx context.Context, key string, value []byte, duration time.Duration) error {
	return r.client.Set(ctx, key, value, duration).Err()
}

func (r *RedisStore) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *RedisStore) CloseConn() error {
	return r.client.Close()
}
