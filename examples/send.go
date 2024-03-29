package main

import (
	"github.com/eininst/rs"
	examples "github.com/eininst/rs/examples/redis"
)

func main() {
	cli := rs.New(examples.GetRedis(), rs.Config{
		//Default "RS_"
		Sender: rs.SenderConfig{
			//Evicts entries as long as the stream's length exceeds the specified threshold
			MaxLen: rs.Int64(100),
		},
	})

	cli.Send("simple", rs.H{
		"title": "this a simple message",
	})
	//
	//cli.SendWithDelay("simple", rs.H{
	//	"title": "this a Delay message",
	//}, time.Second*10)
	//
	//cli.SendWithTime("simple", rs.H{
	//	"title": "this a Time message",
	//}, time.Now().Add(time.Minute*5))
	//
	//cli.Send("test", rs.H{
	//	"something": "hello word",
	//})
	//
	//cli.Send("order_status_change", rs.H{
	//	"order_id": 100,
	//})
}
