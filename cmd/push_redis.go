package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


const (
	DEFAULT_REDIS_HOST					= "127.0.0.1"
	DEFAULT_REDIS_PORT          = "6379"
	DEFAULT_REDIS_PROTOCOL      = "tcp"
)


const (
	TARGET_REDIS            = "redis"
)


const (
	HSET										= "HSET"
	SADD                    = "SADD"
)

const (
  KEY_SEASON							= "season:%s"
	KEY_TEAM                = "team:%s:%s"
	KEY_PLAYER              = "player:%s:%s"
	KEY_GAME                = "game:%s:%s"
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
			transformToRedis()
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


func seasonKey() string {
  return fmt.Sprintf(KEY_SEASON, fFrom)
} // seasonKey


func storeScheduleRedis() {

	s := stats.NbaSchedule{}

	readJson(filepath.Join(fFrom, SCHEDULE_FILENAME), &s)

	rp := RP.Get()

	for _, gd := range s.LeagueSchedule.GameDates {
		
		for _, g := range gd.Games {
			
			log.Println(g)

			_, err := rp.Do(SADD, seasonKey(), g.ID)

			if err != nil {
				log.Println(err)
			}

		}

	}

} // storeScheduleRedis


func storeSeasons() {

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

						if sd.IsDir() {

							games, err := os.ReadDir(filepath.Join(fFrom, d.Name(), sd.Name()))

							if err != nil {
								log.Println(err)
							} else {
	
								for _, g := range games {
	
									if filepath.Ext(g.Name()) == EXT_JSON {

										rp := RP.Get()
										
										b, err := os.ReadFile(filepath.Join(fFrom, d.Name(), sd.Name(), g.Name()))

										if err != nil {
											log.Println(err)
										} else {

											_, err := rp.Do(HSET, sd.Name(), g.Name(), b)

											if err != nil {
												log.Println(err)
											} else {

											}

										}

										rp.Close()

									}
		
								}
		
							}

						}

					}

				}

			}

		}

	}

} // storeSeasons


func transformToRedis() {

	connect(fmt.Sprintf("%s:%s", fHost, fPort))

	storeScheduleRedis()

} // transformToRedis
