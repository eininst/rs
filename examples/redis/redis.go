package examples

import (
	"github.com/go-redis/redis/v8"
	"time"
)

func GetRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		DB:           0,
		DialTimeout:  30 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     100,
		MinIdleConns: 25,
		PoolTimeout:  30 * time.Second,
	})
}
