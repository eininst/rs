package rs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/eininst/flog"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/ivpusic/grpool"
	"math/big"
	"strings"
	"time"
)

var runLog flog.Interface

func init() {
	f := fmt.Sprintf("${time} %s[RS]%s ${msg}  ${fields}", flog.Green, flog.Reset)
	runLog = flog.New(flog.Config{
		Format:    f,
		MsgMinLen: -1,
	})
}

type Context struct {
	context.Context
	Stream     string
	Group      string
	ConsumerId string
	Msg        redis.XMessage
	Ack        func()
}
type Handler func(ctx *Context)

type Msg struct {
	Stream string
	Body   map[string]interface{}
	MaxLen *int64
}

type Rctx struct {
	Stream     string
	Group      string
	Work       *int
	ReadCount  *int64
	BlockTime  time.Duration
	MaxRetries *int64
	Timeout    time.Duration
	Handler    Handler
}

type ReceiveConfig struct {
	Work       *int
	ReadCount  *int64
	BlockTime  time.Duration
	MaxRetries *int64
	Timeout    time.Duration
}

type SenderConfig struct {
	MaxLen *int64
}

type Config struct {
	Prefix  string
	Sender  SenderConfig
	Receive ReceiveConfig
}
type H map[string]interface{}

type Client interface {
	Send(stream string, msg map[string]interface{}) error
	Receive(rctx *Rctx)
	Listen()
	Shutdown()
}

type cancelWrapper struct {
	cancelFunc context.CancelFunc
	pool       *grpool.Pool
}
type client struct {
	Rcli *redis.Client
	Config
	receiveList []*Rctx
	cancelList  []*cancelWrapper
	stop        chan int
}

var (
	DefaultPrefix     = "RS_"
	DefaultReceiveCfg = ReceiveConfig{
		Work:       Int(10),
		ReadCount:  Int64(20),
		BlockTime:  time.Second * 15,
		MaxRetries: Int64(3),
		Timeout:    time.Second * 300,
	}
	DefaultSenderConfig = SenderConfig{MaxLen: nil}
)

func New(rcli *redis.Client, configs ...Config) Client {
	var cfg Config
	if len(configs) > 0 {
		cfg = configs[0]
		if cfg.Prefix == "" {
			cfg.Prefix = DefaultPrefix
		}
		if cfg.Sender == (SenderConfig{}) {
			cfg.Sender = DefaultSenderConfig
		}
		if cfg.Receive == (ReceiveConfig{}) {
			cfg.Receive = DefaultReceiveCfg
		}
		if cfg.Receive.Work == nil {
			flog.Fatal(flog.Red + "ReceiveConfig Work cannot be empty")
		}
		if cfg.Receive.ReadCount == nil {
			flog.Fatal(flog.Red + "ReceiveConfig ReadCount cannot be empty")
		}
		if cfg.Receive.BlockTime == 0 {
			flog.Fatal(flog.Red + "ReceiveConfig BlockTime cannot be empty")
		}
		if cfg.Receive.Timeout == 0 {
			flog.Fatal(flog.Red + "ReceiveConfig Timeout cannot be empty")
		}
		if cfg.Receive.Timeout < time.Second*5 {
			flog.Fatal(flog.Red + "ReceiveConfig Timeout Cannot be less than 5s")
		}
	} else {
		cfg = Config{
			Prefix:  DefaultPrefix,
			Sender:  DefaultSenderConfig,
			Receive: DefaultReceiveCfg,
		}
	}
	return &client{
		Rcli:        rcli,
		Config:      cfg,
		receiveList: []*Rctx{},
		stop:        make(chan int, 1),
	}
}
func (c client) Send(stream string, msg map[string]interface{}) error {
	if msg == nil {
		errMsg := fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", stream)
		flog.Error(errMsg)
		return errors.New(errMsg)
	}
	if len(msg) == 0 {
		errMsg := fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", stream)
		flog.Error(errMsg)
		return errors.New(errMsg)
	}
	ml := int64(0)
	if c.Sender.MaxLen != nil {
		ml = *c.Sender.MaxLen
	}
	return c.Rcli.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: fmt.Sprintf("%s%s", c.Prefix, stream),
		MaxLen: ml,
		Approx: true,
		ID:     "*",
		Values: msg,
	}).Err()
}

func (c *client) Receive(rctx *Rctx) {
	if rctx.Stream == "" {
		flog.Panic("Receive Stream cannot be empty")
	}
	if rctx.Handler == nil {
		flog.Panic("Receive Handler cannot be empty")
	}
	if rctx.Work == nil {
		rctx.Work = c.Config.Receive.Work
	}
	if rctx.ReadCount == nil {
		rctx.ReadCount = c.Config.Receive.ReadCount
	}
	if rctx.BlockTime == 0 {
		rctx.BlockTime = c.Config.Receive.BlockTime
	}
	if rctx.Timeout == 0 {
		rctx.Timeout = c.Config.Receive.Timeout
	}
	if rctx.Timeout < time.Second*5 {
		flog.Fatal(flog.Red + "ReceiveConfig Timeout Cannot be less than 5s")
	}
	if rctx.MaxRetries == nil {
		rctx.MaxRetries = c.Config.Receive.MaxRetries
	}

	rctx.Stream = fmt.Sprintf("%s%s", c.Prefix, rctx.Stream)
	c.receiveList = append(c.receiveList, rctx)
}

func (c *client) Listen() {
	ctx := context.Background()
	for _, v := range c.receiveList {
		rctx := v

		c.runInfoLog(rctx)
		go func() {
			c.Rcli.XGroupCreateMkStream(ctx, rctx.Stream, rctx.Group, "0")
			pool := grpool.NewPool(*rctx.Work, *rctx.Work)
			consumerId := uuid.NewString()
			ctxls, cancel := context.WithCancel(ctx)

			c.cancelList = append(c.cancelList, &cancelWrapper{
				cancelFunc: cancel,
				pool:       pool,
			})

			go func() {
				c.retries(ctxls, pool, consumerId, rctx)
			}()
			c.listenStream(ctxls, pool, consumerId, rctx)
		}()

	}
	<-c.stop
}

func (c *client) Shutdown() {
	defer func() { c.stop <- 1 }()
	for _, v := range c.cancelList {
		v.cancelFunc()
	}
	for _, v := range c.cancelList {
		v.pool.Release()
	}
}

func (c *client) listenStream(ctx context.Context, pool *grpool.Pool, consumerId string, rctx *Rctx) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			entries, err := c.Rcli.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    rctx.Group,
				Consumer: consumerId,
				Streams:  []string{rctx.Stream, ">"},
				Count:    *rctx.ReadCount,
				Block:    rctx.BlockTime,
				NoAck:    false,
			}).Result()

			if err != nil {
				continue
			}
			if len(entries) == 0 {
				continue
			}
			msgs := entries[0].Messages
			for _, msg := range msgs {
				pool.JobQueue <- func() {
					defer func() {
						if err := recover(); err != nil {
							flog.Errorf("subject:%v, err:%v", rctx.Stream, err)
						}
					}()

					ctx := context.Background()
					rctx.Handler(&Context{
						Context:    ctx,
						Stream:     rctx.Stream,
						Group:      rctx.Group,
						ConsumerId: consumerId,
						Msg:        msg,
						Ack: func() {
							_, e := c.Rcli.XAck(ctx, rctx.Stream, rctx.Group, msg.ID).Result()
							if e == nil {
								c.Rcli.XDel(ctx, rctx.Stream, msg.ID)
							}
						},
					})

				}
			}
		}
	}
}
func (c *client) retries(ctx context.Context, pool *grpool.Pool, consumerId string, rctx *Rctx) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, _ := rand.Int(rand.Reader, big.NewInt(1000))
			time.Sleep(time.Millisecond*500 + time.Duration(n.Int64()))

			ok, er := c.Rcli.SetNX(ctx, c.Prefix+"retries:"+rctx.Stream, "1", time.Millisecond*1500).Result()
			if er != nil {
				continue
			}
			if !ok {
				continue
			}
			pcmds, err := c.Rcli.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: rctx.Stream,
				Group:  rctx.Group,
				Idle:   rctx.Timeout,
				Start:  "0",
				End:    "+",
				Count:  *rctx.ReadCount,
			}).Result()
			if err != nil {
				flog.Error("XPendingExt Error:", err)
				continue
			}
			xdel_ids := []string{}
			for _, cmd := range pcmds {
				if cmd.RetryCount > *rctx.MaxRetries {
					xdel_ids = append(xdel_ids, cmd.ID)
				} else {
					xmsgs, err := c.Rcli.XRangeN(ctx, rctx.Stream, cmd.ID, cmd.ID, 1).Result()
					if err != nil {
						flog.Error("XRangeN Error:", err)
					}
					if len(xmsgs) > 0 {
						c.Rcli.XClaim(ctx, &redis.XClaimArgs{
							Stream:   rctx.Stream,
							Group:    rctx.Group,
							Consumer: cmd.Consumer,
							MinIdle:  0,
							Messages: []string{cmd.ID},
						})
						msg := xmsgs[0]
						pool.JobQueue <- func() {
							defer func() {
								if err := recover(); err != nil {
									flog.Errorf("subject:%v, err:%v", rctx.Stream, err)
								}
							}()

							rctx.Handler(&Context{
								Context:    context.Background(),
								Stream:     rctx.Stream,
								Group:      rctx.Group,
								ConsumerId: consumerId,
								Msg:        msg,
								Ack: func() {
									_, e := c.Rcli.XAck(ctx, rctx.Stream, rctx.Group, msg.ID).Result()
									if e == nil {
										c.Rcli.XDel(ctx, rctx.Stream, msg.ID)
									}
								},
							})
						}
					}
				}
			}

			if len(xdel_ids) > 0 {
				c.Rcli.XAck(ctx, rctx.Stream, rctx.Group, xdel_ids...)
				c.Rcli.XDel(ctx, rctx.Stream, xdel_ids...)
			}
		}
	}
}

func (c *client) runInfoLog(rctx *Rctx) {
	name := strings.Replace(rctx.Stream, c.Prefix, "", -1)
	name = fmt.Sprintf("%s%s%s", flog.Green, name, flog.Reset)
	if rctx.Group != "" {
		name += fmt.Sprintf(":%s%s%s", flog.White, rctx.Group, flog.Reset)
	}
	runLog.With(flog.Fields{
		"Work":       *rctx.Work,
		"MaxRetries": *rctx.MaxRetries,
		"Timeout":    rctx.Timeout,
		"ReadCount":  *rctx.ReadCount,
		"BlockTime":  rctx.BlockTime,
	}).Infof("Stream \"%s\" working... ", name)
}
