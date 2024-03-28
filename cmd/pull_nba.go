package cmd

import (
	"fmt"
	//"log"
	"strings"

	"github.com/madsportslab/nbalake"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	schedule			*stats.NbaSchedule

	pullNbaCmd = &cobra.Command{
		Use: "nba",
		Short: "nba statistics",
		Long: "nba statistics from the official NBA APIs",
		Run: func(cmd *cobra.Command, args []string) {
			pullBoxscores()
		},
	}

)


var ScheduleIndex map[string] bool
var PlaysIndex map[string] bool

var rawBlobs <-chan minio.ObjectInfo


func init() {
} // init


func ScheduleEndpoint() string {
	
	return fmt.Sprintf("%s%s%s",
		stats.NBA_BASE_URL,
		stats.NBA_STATIC,
		stats.NBA_SCHEDULE,
	)

} // ScheduleEndpoint


func LoadBlobIndexes() {

	ScheduleIndex 	= make(map[string] bool)
	PlaysIndex 			= make(map[string] bool)

	rb := nbalake.BucketName(cy, nbalake.BUCKET_RAW)

	ab := nbalake.BucketName(cy, nbalake.BUCKET_ANALYTICS)

	nbalake.InitBuckets([]string{rb, ab})

	rawBlobs = nbalake.List(rb)

	for b := range rawBlobs {

		name := strings.TrimSuffix(b.Key, EXT_JSON)

		if strings.Contains(b.Key, PBP_SUFFIX) {

			id := strings.TrimSuffix(name, EXT_PBP)

			PlaysIndex[id] = true

		} else if b.Key != SCHEDULE_BLOB {
			ScheduleIndex[name] = true
		}

	}

} // LoadBlobIndexes


func loadSchedule(y string) {

	bucket := nbalake.BucketName(y,
		nbalake.BUCKET_RAW)

	blob := nbalake.Get(bucket, SCHEDULE_BLOB)	
	
	if len(blob) == 0 {

		buf := stats.NbaGetScheduleJson()

		nbalake.Put(bucket, SCHEDULE_BLOB, buf)

	}
	
	read(&schedule, bucket, SCHEDULE_BLOB)

} // loadSchedule


func pullAll() {

	for _, day := range schedule.LeagueSchedule.GameDates {

		if !stats.IsFutureGame(day.GameDate) {

			for _, game := range day.Games {
				
				//if game.WeekNumber > 0 {
					
					_, ok := ScheduleIndex[game.ID]

					if !ok {

						box := stats.NbaGetBoxscoreJson(game.ID)

						fn 	:= fmt.Sprintf("%s.json", game.ID)
						
						nbalake.Put(nbalake.BucketName(cy,
							nbalake.BUCKET_RAW), fn, box)
						
						ScheduleIndex[game.ID] = true

					}

					_, ok = PlaysIndex[game.ID] 

					if !ok {

						fn := fmt.Sprintf("%s.playbyplay.json", game.ID)

						plays := stats.NbaGetPlaysJson(game.ID)

						nbalake.Put(nbalake.BucketName(cy, nbalake.BUCKET_RAW), fn, plays)

						PlaysIndex[game.ID] = true

					}
					
				//}
	
			}

		}

	}

} // pullAll


func pullBoxscores() {
	
	loadSchedule(cy)

	LoadBlobIndexes()

	pullAll()

} // pullBoxscores
