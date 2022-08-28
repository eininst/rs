package main

import (
	"github.com/eininst/rs"
	examples "github.com/eininst/rs/examples/redis"
)

func main() {
	cli := rs.New(examples.GetRedis())

	cli.Send(&rs.Msg{
		Stream: "simple",
		Body: rs.H{
			"title": "this a simple message",
		},
	})

	cli.Send(&rs.Msg{
		Stream: "test",
		Body: rs.H{
			"something": "hello word",
		},
	})

	cli.Send(&rs.Msg{
		Stream: "order_status_change",
		Body: rs.H{
			"order_id": 100,
		},
	})
}
