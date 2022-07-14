package cmd

import (
	"fmt"
	//"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

	fHost           string
	fPort           string

	redisCmd = &cobra.Command{
		Use: "redis",
		Short: "redis data store",
		Long: "redis data store for statistics",
		Run: func(cmd *cobra.Command, args []string) {
			loadData()
		},
	}

)


func init() {

	pushCmd.Flags().StringVarP(&fHost, "host", "", DEFAULT_REDIS_HOST,
		"Data store host address")
	pushCmd.Flags().StringVarP(&fPort, "port", "p", DEFAULT_REDIS_PORT,
		"Data store address port")

} // init


func connect(addr string) {

	RP = &redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {return redis.Dial(
			DEFAULT_REDIS_PROTOCOL, addr)},
	}

} // connect


func readSeasons() {

	dirs, err := os.ReadDir(fFrom)

	if err != nil {
		log.Println(err)
	} else {

		for _, d := range dirs {

			if d.IsDir() {

				sdirs, err := os.ReadDir(filepath.Join(fFrom, d.Name()))

				if err != nil {
					log.Println(err)
				} else {

					for _, sd := range sdirs {

						games, err := os.ReadDir(filepath.Join(fFrom, d.Name(), sd.Name()))

						if err != nil {
							log.Println(err)
						} else {

							for _, g := range games {

								if filepath.Ext(g.Name()) == EXT_JSON {
									log.Println(g.Name())
								}
	
							}
	
						}

					}

				}
			}

		}

	}


} // readSeasons


func loadData() {

	connect(fmt.Sprintf("%s:%s", fHost, fPort))

	readSeasons()

} // loadData
