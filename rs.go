package rs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eininst/flog"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/ivpusic/grpool"
	"github.com/robfig/cron/v3"
	"github.com/tidwall/gjson"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var runLog flog.Interface

func init() {
	f := fmt.Sprintf("${time} ${level} %s[RS]%s ${msg} # ${fields}", flog.Green, flog.Reset)
	runLog = flog.New(flog.Config{
		Format: f,
	})
}

type Context struct {
	context.Context
	Stream     string
	Group      string
	ConsumerId string
	Msg        redis.XMessage
	JSON       gjson.Result
	Client     *redis.Client
	Delay      time.Duration
	Ack        func()
}
type Call func(ctx *Context)

type Msg struct {
	Stream string
	Body   map[string]interface{}
	MaxLen *int64
}

type Option struct {
	F func(o *Options)
}

type Options struct {
	Group      string
	Work       *int
	ReadCount  *int64
	BlockTime  time.Duration
	MaxRetries *int64
	Timeout    time.Duration
}

func (o *Options) Apply(opts []Option) {
	for _, op := range opts {
		op.F(o)
	}
}

type Rctx struct {
	Stream     string
	Group      string
	Work       *int
	ReadCount  *int64
	BlockTime  time.Duration
	MaxRetries *int64
	Timeout    time.Duration
	Handler    Call
}

type ReceiveConfig struct {
	Work           *int          `json:"work"`
	ReadCount      *int64        `json:"readCount"`
	BlockTime      time.Duration `json:"blockTime"`
	MaxRetries     *int64        `json:"maxRetries"`
	Timeout        time.Duration `json:"timeout"`
	ZRangeInterval time.Duration `json:"zRangeInterval"`
}

type SenderConfig struct {
	MaxLen *int64 `json:"maxLen"`
}

type Config struct {
	Prefix  string        `json:"prefix"`
	Sender  SenderConfig  `json:"sender"`
	Receive ReceiveConfig `json:"receive"`
}
type H map[string]interface{}

type Client interface {
	Send(stream string, msg map[string]interface{}) error
	SendWithDelay(stream string, msg map[string]interface{}, delay time.Duration) error
	SendWithTime(stream string, msg map[string]interface{}, datetime time.Time) error
	CronSend(spec string, stream string)

	Handler(stream string, call Call, opts ...Option)
	Receive(rctx Rctx)
	Listen()
	Shutdown()
	Start()
}

type cancelWrapper struct {
	cancelFunc context.CancelFunc
	pool       *grpool.Pool
}

type client struct {
	Rcli *redis.Client
	Config
	receiveList []Rctx
	cancelList  []*cancelWrapper
	stop        chan int
	cronCli     *cron.Cron
}

const zgetAndRem = `
local items = redis.call("zrangebyscore", KEYS[1],0,ARGV[1],"limit",0,1)
if #items == 0 then
    return ""
else
	redis.call('zremrangebyrank', KEYS[1],0,0)
    return items[1]
end`

var (
	mux               = &sync.Mutex{}
	DefaultPrefix     = "RS_"
	DefaultReceiveCfg = ReceiveConfig{
		Work:           Int(10),
		ReadCount:      Int64(20),
		BlockTime:      time.Second * 15,
		MaxRetries:     Int64(3),
		Timeout:        time.Second * 300,
		ZRangeInterval: time.Millisecond * 500,
	}
	DefaultSenderConfig  = SenderConfig{MaxLen: Int64(2048)}
	zgetAndRemHash       = ""
	zgetAndRemHashUpdate = false
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
		} else {
			if cfg.Sender.MaxLen == nil {
				cfg.Sender.MaxLen = DefaultSenderConfig.MaxLen
			}
		}
		if cfg.Receive == (ReceiveConfig{}) {
			cfg.Receive = DefaultReceiveCfg
		} else {
			if cfg.Receive.Work == nil {
				cfg.Receive.Work = DefaultReceiveCfg.Work
			}
			if cfg.Receive.ReadCount == nil {
				cfg.Receive.ReadCount = DefaultReceiveCfg.ReadCount
			}
			if cfg.Receive.BlockTime == 0 {
				cfg.Receive.BlockTime = DefaultReceiveCfg.BlockTime
			}
			if cfg.Receive.Timeout == 0 {
				cfg.Receive.Timeout = DefaultReceiveCfg.Timeout
			}
			if cfg.Receive.Timeout < time.Second*5 {
				flog.Fatal(flog.Red + "ReceiveConfig Timeout Cannot be less than 5s")
			}
			if cfg.Receive.ZRangeInterval == 0 {
				cfg.Receive.ZRangeInterval = DefaultReceiveCfg.ZRangeInterval
			} else if cfg.Receive.ZRangeInterval < time.Millisecond*5 {
				flog.Fatal(flog.Red + "ReceiveConfig ZRangeInterval Cannot be less than 5ms")
			}
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
		receiveList: []Rctx{},
		stop:        make(chan int, 1),
		cronCli:     cron.New(cron.WithSeconds()),
	}
}

func (c client) CronSend(spec string, stream string) {
	key := fmt.Sprintf("cron_lock:%v", stream)
	_, er := c.cronCli.AddFunc(spec, func() {
		_ctx := context.Background()
		ok, er := c.Rcli.SetNX(_ctx, key, "1", time.Second).Result()
		if er != nil {
			flog.Errorf(`SetNX key: "%s", Error: %v`, key, er)
			return
		}
		if ok {
			err := c.Send(stream, H{
				"nonce": uuid.NewString(),
			})
			if err != nil {
				flog.Error(er)
			}
		}
	})
	if er != nil {
		flog.Fatal(er)
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

	err := c.Rcli.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: fmt.Sprintf("%s%s", c.Prefix, stream),
		MaxLen: ml,
		Approx: true,
		ID:     "*",
		Values: msg,
	}).Err()

	if err != nil {
		flog.Errorf("Send msg err by %v, err:%v", stream, err)
	}
	return err
}

func (c client) SendWithDelay(stream string, msg map[string]interface{}, delay time.Duration) error {
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
	if delay == 0 {
		errMsg := fmt.Sprintf("Delay Cannot be 0 by Stream \"%s\"", stream)
		flog.Error(errMsg)
		return errors.New(errMsg)
	}

	addr := fmt.Sprintf("z_%s%s", c.Prefix, stream)
	msgb, er := json.Marshal(msg)
	if er != nil {
		errMsg := fmt.Sprintf("Send msg Cannot be Marshal by Stream \"%s\"", stream)
		flog.Error(errMsg)
	}

	score := time.Now().UnixMilli() + int64(delay/1000000)
	err := c.Rcli.ZAdd(context.TODO(), addr, &redis.Z{
		Score:  float64(score),
		Member: msgb,
	}).Err()

	if err != nil {
		flog.Errorf("SendWithDelay err by %v, err:%v", stream, err)
	}

	return err
}

func (c client) SendWithTime(stream string, msg map[string]interface{}, datetime time.Time) error {
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

	score := datetime.UnixMilli()
	if score < time.Now().UnixMilli() {
		errMsg := fmt.Sprintf("Datetime Cannot be less than or equal to the current time by \"%s\"", stream)
		flog.Error(errMsg)
		return errors.New(errMsg)
	}

	addr := fmt.Sprintf("z_%s%s", c.Prefix, stream)
	msgb, er := json.Marshal(msg)
	if er != nil {
		errMsg := fmt.Sprintf("Send msg Cannot be Marshal by Stream \"%s\"", stream)
		flog.Error(errMsg)
	}

	err := c.Rcli.ZAdd(context.TODO(), addr, &redis.Z{
		Score:  float64(score),
		Member: msgb,
	}).Err()

	if err != nil {
		flog.Errorf("SendWithDelay err by %v, err:%v", stream, err)
	}

	return err
}

func (c *client) Handler(stream string, call Call, opts ...Option) {
	options := &Options{
		Group:      "",
		Work:       c.Config.Receive.Work,
		ReadCount:  c.Config.Receive.ReadCount,
		BlockTime:  c.Config.Receive.BlockTime,
		MaxRetries: c.Config.Receive.MaxRetries,
		Timeout:    c.Config.Receive.Timeout,
	}
	options.Apply(opts)

	c.Receive(Rctx{
		Stream:     stream,
		Handler:    call,
		Group:      options.Group,
		Work:       options.Work,
		ReadCount:  options.ReadCount,
		BlockTime:  options.BlockTime,
		MaxRetries: options.MaxRetries,
		Timeout:    options.Timeout,
	})
}

func (c *client) Receive(rctx Rctx) {
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

func (c *client) Start() {
	go func() {
		quit := make(chan os.Signal)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM)
		<-quit
		flog.Info("Start shutdown...")
		cli.Shutdown()
		flog.Info("Graceful shutdown success!")
	}()
	c.Listen()
}

func (c *client) Listen() {
	ctx := context.Background()
	zhash, err := c.Rcli.ScriptLoad(ctx, zgetAndRem).Result()
	if err != nil {
		flog.Warn("[RS] Script load error:", err)
	} else {
		mux.Lock()
		zgetAndRemHash = zhash
		mux.Unlock()
	}

	for _, v := range c.receiveList {
		rctx := v

		c.runInfoLog(rctx)
		go func() {
			c.Rcli.XGroupCreateMkStream(ctx, rctx.Stream, rctx.Group, "0")
			pool := grpool.NewPool(*rctx.Work, 0)
			consumerId := uuid.NewString()
			ctxls, cancel := context.WithCancel(ctx)

			c.cancelList = append(c.cancelList, &cancelWrapper{
				cancelFunc: cancel,
				pool:       pool,
			})

			go c.zrangeByScore(ctxls, rctx)

			go c.retries(ctxls, pool, consumerId, rctx)

			c.listenStream(ctxls, pool, consumerId, rctx)
		}()
	}
	c.cronCli.Start()

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

func WithGroup(group string) Option {
	return Option{F: func(o *Options) {
		o.Group = group
	}}
}

func WithWork(work int) Option {
	return Option{F: func(o *Options) {
		o.Work = Int(work)
	}}
}

func WithReadCount(readCount int64) Option {
	return Option{F: func(o *Options) {
		o.ReadCount = Int64(readCount)
	}}
}

func WithBlockTime(blockTime time.Duration) Option {
	return Option{F: func(o *Options) {
		o.BlockTime = blockTime
	}}
}

func WithMaxRetries(maxRetries int64) Option {
	return Option{F: func(o *Options) {
		if maxRetries == 0 {
			o.MaxRetries = nil
		} else {
			o.MaxRetries = Int64(maxRetries)
		}
	}}
}

func WithTimeout(timeout time.Duration) Option {
	return Option{F: func(o *Options) {
		o.Timeout = timeout
	}}
}

func (c *client) listenStream(ctx context.Context, pool *grpool.Pool, consumerId string, rctx Rctx) {
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
				if errors.Is(err, redis.Nil) {
					continue
				} else {
					time.Sleep(time.Second * 2)
					continue
				}
			}
			if len(entries) == 0 {
				continue
			}
			msgs := entries[0].Messages
			for _, msg := range msgs {
				pool.JobQueue <- func() {
					defer func() {
						if err := recover(); err != nil {
							flog.Errorf("subject:%v, err:%v, stack=%v\n", rctx.Stream, err, string(debug.Stack()))
						}
					}()

					ctx := context.Background()
					value, _ := json.Marshal(msg.Values)

					rctx.Handler(&Context{
						Context:    ctx,
						Stream:     rctx.Stream,
						Group:      rctx.Group,
						ConsumerId: consumerId,
						Msg:        msg,
						JSON:       gjson.ParseBytes(value),
						Client:     c.Rcli,
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

func (c *client) retries(ctx context.Context, pool *grpool.Pool, consumerId string, rctx Rctx) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pcmds, err := c.Rcli.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: rctx.Stream,
				Group:  rctx.Group,
				Idle:   rctx.Timeout,
				Start:  "0",
				End:    "+",
				Count:  *rctx.ReadCount,
			}).Result()
			if err != nil {
				flog.Error(err)
				time.Sleep(time.Second * 3)
				continue
			}
			if len(pcmds) == 0 {
				time.Sleep(time.Second)
				continue
			}

			consumerMap := map[string][]string{}
			xdel_ids := []string{}

			for _, cmd := range pcmds {
				if cmd.RetryCount > *rctx.MaxRetries {
					xdel_ids = append(xdel_ids, cmd.ID)
				} else {
					if v, ok := consumerMap[cmd.Consumer]; ok {
						v = append(v, cmd.ID)
						consumerMap[cmd.Consumer] = v
					} else {
						consumerMap[cmd.Consumer] = []string{cmd.ID}
					}
				}
			}

			if len(xdel_ids) > 0 {
				c.Rcli.XAck(ctx, rctx.Stream, rctx.Group, xdel_ids...)
				c.Rcli.XDel(ctx, rctx.Stream, xdel_ids...)
			}

			if len(consumerMap) > 0 {
				for consumer, msgIds := range consumerMap {
					xmsgs, er := c.Rcli.XClaim(ctx, &redis.XClaimArgs{
						Stream:   rctx.Stream,
						Group:    rctx.Group,
						Consumer: consumer,
						MinIdle:  rctx.Timeout,
						Messages: msgIds,
					}).Result()

					if er != nil {
						flog.Error("XRangeN Error:", er)
					}

					for _, msg := range xmsgs {
						pool.JobQueue <- func() {
							defer func() {
								if err := recover(); err != nil {
									flog.Errorf("subject:%v, err:%v, stack=%v\n", rctx.Stream, err, string(debug.Stack()))
								}
							}()

							value, _ := json.Marshal(msg.Values)

							rctx.Handler(&Context{
								Context:    context.Background(),
								Stream:     rctx.Stream,
								Group:      rctx.Group,
								ConsumerId: consumerId,
								Client:     c.Rcli,
								Msg:        msg,
								JSON:       gjson.ParseBytes(value),
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
	}
}

func (c *client) zrangeByScore(ctx context.Context, rctx Rctx) {
	key := fmt.Sprintf("z_%s", rctx.Stream)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			val := strconv.FormatInt(time.Now().UnixMilli(), 10)
			var r interface{}
			var err error
			if zgetAndRemHash != "" {
				r, err = c.Rcli.EvalSha(ctx, zgetAndRemHash, []string{key}, []any{val}).Result()
			} else {
				r, err = c.Rcli.Eval(ctx, zgetAndRem, []string{key}, []any{val}).Result()
			}
			if err != nil {
				flog.Error(err)
				if err.Error() == "NOSCRIPT No matching script. Please use EVAL." {
					mux.Lock()
					if !zgetAndRemHashUpdate {
						zhash, err := c.Rcli.ScriptLoad(ctx, zgetAndRem).Result()
						if err != nil {
							zgetAndRemHash = ""
						} else {
							zgetAndRemHash = zhash
						}
						zgetAndRemHashUpdate = true
					}
					mux.Unlock()

					continue
				} else {
					zgetAndRemHashUpdate = false
					time.Sleep(time.Second * 2)

					continue
				}
			}
			if reply := r.(string); reply != "" {
				var m map[string]interface{}
				_ = json.Unmarshal([]byte(reply), &m)
				_ = c.Rcli.XAdd(ctx, &redis.XAddArgs{
					Stream: rctx.Stream,
					Approx: true,
					ID:     "*",
					Values: m,
				}).Err()
			} else {
				time.Sleep(c.Config.Receive.ZRangeInterval)
			}
		}
	}
}

func (c *client) runInfoLog(rctx Rctx) {
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
	}).Infof("Stream \"%s\" working...", name)
}
