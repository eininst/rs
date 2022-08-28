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
		//default configuration for receiving messages
		Receive: rs.ReceiveConfig{
			Work:       rs.Int(10),       //Per stream goroutine number,
			Timeout:    time.Second * 20, //Retry after timeout
			MaxRetries: rs.Int64(3),      //Max retries
			ReadCount:  rs.Int64(50),     //XReadGroup Count
			BlockTime:  time.Second * 20, //XReadGroup Block Time
		},
	})

	cli.Receive(&rs.Rctx{
		Stream: "simple",
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			jstr, _ := json.Marshal(ctx.Msg.Values)
			flog.Info("received simple msg:", string(jstr))
		},
	})

	cli.Receive(&rs.Rctx{
		Stream:     "test",
		MaxRetries: nil, //no retries
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			jstr, _ := json.Marshal(ctx.Msg.Values)
			flog.Info("received test msg:", string(jstr))
		},
	})

	cli.Receive(&rs.Rctx{
		Stream:     "order_status_change",
		Work:       rs.Int(20),
		Timeout:    time.Second * 120,
		MaxRetries: rs.Int64(6),
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			orderId := ctx.Msg.Values["order_id"]
			flog.Info("received order_status_change msg:", orderId)
		},
	})

	cli.Listen()
}
