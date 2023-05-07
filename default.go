package rs

import (
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

var cli Client
var once sync.Once

func SetDefault(rcli *redis.Client, configs ...Config) {
	once.Do(func() {
		cli = New(rcli, configs...)
	})
}

func Send(stream string, msg map[string]interface{}) error {
	return cli.Send(stream, msg)
}

func SendWithDelay(stream string, msg map[string]interface{}, delay time.Duration) error {
	return cli.SendWithDelay(stream, msg, delay)
}

func SendWithTime(stream string, msg map[string]interface{}, datetime time.Time) error {
	return cli.SendWithTime(stream, msg, datetime)
}

func CronSend(spec string, stream string) {
	cli.CronSend(spec, stream)
}

func Receive(rctx Rctx) {
	cli.Receive(rctx)
}

func Listen() {
	cli.Listen()
}

func Shutdown() {
	cli.Shutdown()
}
