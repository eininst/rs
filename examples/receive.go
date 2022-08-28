package main

import (
	"encoding/json"
	"github.com/eininst/flog"
	"github.com/eininst/rs"
	examples "github.com/eininst/rs/examples/redis"
	"time"
)

func main() {
	cli := rs.New(examples.GetRedis(), rs.Config{
		Receive: rs.ReceiveConfig{
			Work:       rs.Int(10),
			Timeout:    time.Second * 20,
			MaxRetries: rs.Int64(3),
			ReadCount:  rs.Int64(50),
			BlockTime:  time.Second * 20,
		},
	})

	cli.Receive(&rs.Rctx{
		Stream: "test",
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			jstr, _ := json.Marshal(ctx.Msg.Values)
			flog.Info("test received msg:", string(jstr))
		},
	})

	cli.Receive(&rs.Rctx{
		Stream: "order_status_change",
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			orderId := ctx.Msg.Values["order_id"]

			flog.Info("order_status_change received msg:", orderId)
		},
	})

	cli.Listen()
}
