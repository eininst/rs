package main

import (
	"encoding/json"
	"github.com/eininst/flog"
	"github.com/eininst/rs"
	examples "github.com/eininst/rs/examples/redis"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cli := rs.New(examples.GetRedis(), rs.Config{
		//default configuration for receiving messages
		Receive: rs.ReceiveConfig{
			Work:       rs.Int(10),        //Per stream goroutine number,
			Timeout:    time.Second * 300, //Retry after timeout
			MaxRetries: rs.Int64(5),       //Max retries
			ReadCount:  rs.Int64(50),      //XReadGroup Count
			BlockTime:  time.Second * 10,  //XReadGroup Block Time
		},
	})

	cli.Handler("simple", func(ctx *rs.Context) {
		defer ctx.Ack()
		flog.Info(ctx.JSON.Raw)
	})

	cli.Receive(rs.Rctx{
		Stream: "simple",
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			flog.Info(ctx.Msg)
		},
	})

	cli.Receive(rs.Rctx{
		Stream:     "test",
		Group:      "group1",
		MaxRetries: nil, //no retries
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			jstr, _ := json.Marshal(ctx.Msg.Values)
			flog.Info("received test msg:", string(jstr))
		},
	})

	cli.Receive(rs.Rctx{
		Stream:     "test",
		Group:      "group2",
		MaxRetries: nil, //no retries
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			jstr, _ := json.Marshal(ctx.Msg.Values)
			flog.Info("received test msg:", string(jstr))
		},
	})

	cli.Receive(rs.Rctx{
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

	go func() {
		quit := make(chan os.Signal)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit

		cli.Shutdown()
		flog.Info("Graceful shutdown")
	}()

	cli.Listen()
}
