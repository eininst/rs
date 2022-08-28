# RS

[![Build Status](https://travis-ci.org/ivpusic/grpool.svg?branch=master)](https://github.com/infinitasx/easi-go-aws)

`RS` 基于Redis Streams 实现的golang版本消息队列

## Installation
```text
go get -u github.com/eininst/rs
```
## ⚡ Quickstart

### Init

```go
rcli := rs.New(rcli *redis.Client)
```


### Send a message
```go
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
```

See [examples](/examples)

## License
*MIT*