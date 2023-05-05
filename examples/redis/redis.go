package examples

import (
	"github.com/go-redis/redis/v8"
	"time"
)

//redis:
//addr: r-2vcqsrknxl3fef635ypd.redis.cn-chengdu.rds.aliyuncs.com:6379
//password: 7c3cD505
//db: 1
//poolSize: 100
//minIdleCount: 20

func GetRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		//Password:     "7c3cD505",
		DB:           0,
		DialTimeout:  30 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     100,
		MinIdleConns: 25,
		PoolTimeout:  30 * time.Second,
	})
}
