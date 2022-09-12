package main

import (
	"github.com/eininst/rs"
	examples "github.com/eininst/rs/examples/redis"
	"time"
)

func main() {
	cli := rs.New(examples.GetRedis(), rs.Config{
		Sender: rs.SenderConfig{
			//Evicts entries as long as the stream's length exceeds the specified threshold
			MaxLen: rs.Int64(100),
		},
	})

	cli.Send("simple", rs.H{
		"title": "this a simple message",
	})

	cli.SendWithTime("simple", rs.H{
		"title": "this a simple message2",
	}, time.Now().Add(time.Minute*5))

	cli.Send("test", rs.H{
		"something": "hello word",
	})

	cli.Send("order_status_change", rs.H{
		"order_id": 100,
	})
}
