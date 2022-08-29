# RS

[![Build Status](https://travis-ci.org/ivpusic/grpool.svg?branch=master)](https://github.com/infinitasx/easi-go-aws)

`RS` Is a message queue for redis streams

## Installation

```text
go get -u github.com/eininst/rs
```

## ⚡ Quickstart

```go
cli := rs.New(rcli *redis.Client)
```

> You can customize it all you want:

```go
cli := rs.New(examples.GetRedis(), rs.Config{
    Sender: rs.SenderConfig{
        //Evicts entries as long as the stream's length exceeds the specified threshold
        MaxLen: rs.Int64(100),
    },
})
```

## Send a message

```go
cli.Send("simple", rs.H{
    "title": "this a simple message",
})

cli.Send("test", rs.H{
    "something": "hello word",
})

cli.Send("order_status_change", rs.H{
    "order_id": 100,
})
```

## Receive message

```go
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
		Group:      "group1",
		MaxRetries: nil, //no retries
		Handler: func(ctx *rs.Context) {
			defer ctx.Ack()
			jstr, _ := json.Marshal(ctx.Msg.Values)
			flog.Info("received test msg:", string(jstr))
		},
	})

	cli.Receive(&rs.Rctx{
		Stream:     "test",
		Group:      "group2",
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

	go func() {
		quit := make(chan os.Signal)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit

		cli.Shutdown()
		flog.Info("Graceful shutdown")
	}()

	cli.Listen()
}
```

```text
2022/08/29 20:25:22 [RS] Stream "simple" working...   BlockTime=20s MaxRetries=3 ReadCount=50 Timeout=20s Work=10
2022/08/29 20:25:22 [RS] Stream "test:group1" working...   BlockTime=20s MaxRetries=3 ReadCount=50 Timeout=20s Work=10
2022/08/29 20:25:22 [RS] Stream "test:group2" working...   BlockTime=20s MaxRetries=3 ReadCount=50 Timeout=20s Work=10
2022/08/29 20:25:22 [RS] Stream "order_status_change" working...   BlockTime=20s MaxRetries=6 ReadCount=50 Timeout=2m0s Work=20
2022/08/29 20:25:27 [INFO] receive.go:31 received simple msg: {"title":"this a simple message"} 
2022/08/29 20:25:27 [INFO] receive.go:53 received test msg: {"something":"hello word"}  
2022/08/29 20:25:27 [INFO] receive.go:42 received test msg: {"something":"hello word"}  
2022/08/29 20:25:27 [INFO] receive.go:65 received order_status_change msg: 100 
```

> Graceful Shutdown

```go
go func () {
    quit := make(chan os.Signal)
    signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
    <-quit
    
    cli.Shutdown()
    flog.Info("Graceful Shutdown")
}()

cli.Listen()
```

> See [examples](/examples)

## License

*MIT*