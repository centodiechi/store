package store

import (
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
	return nil, nil
}
