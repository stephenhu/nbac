package cmd

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/spf13/cobra"
)


const (
	DEFAULT_REDIS_HOST					= "127.0.0.1"
	DEFAULT_REDIS_PORT          = "6379"
	DEFAULT_REDIS_PROTOCOL      = "tcp"
)


const (
	TARGET_REDIS            = "redis"
)


var RP *redis.Pool


var (

	redisCmd = &cobra.Command{
		Use: "redis",
		Short: "redis data store",
		Long: "redis data store for statistics",
		Run: func(cmd *cobra.Command, args []string) {
			loadData()
		},
	}

)

func connect(addr string) {

	RP = &redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {return redis.Dial(
			DEFAULT_REDIS_PROTOCOL, addr)},
	}

} // connect


func loadData() {

	connect(fmt.Sprintf("%s:%s", fHost, fPort))

} // loadData
