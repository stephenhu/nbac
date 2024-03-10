package cmd

import (
	"fmt"
	//"log"

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


func init() {
} // init


func ScheduleEndpoint() string {
	
	return fmt.Sprintf("%s%s%s",
		stats.NBA_BASE_URL,
		stats.NBA_STATIC,
		stats.NBA_SCHEDULE,
	)

} // ScheduleEndpoint


func loadSchedule(y string) {

	bucket := BucketRaw(y)

	blob := BlobGet(bucket, SCHEDULE_BLOB)	
	
	if len(blob) == 0 {

		buf := stats.NbaGetScheduleJson()

		BlobPut(bucket, SCHEDULE_BLOB, buf)

	}
	
	read(&schedule, bucket, SCHEDULE_BLOB)

} // loadSchedule


func pullAll() {

	for _, day := range schedule.LeagueSchedule.GameDates {

		if !stats.IsFutureGame(day.GameDate) {

			for _, game := range day.Games {
				
				if game.WeekNumber > 0 {
					
					_, ok := ScheduleMap[game.ID]

					if !ok {

						box := stats.NbaGetBoxscoreJson(game.ID)

						fn 	:= fmt.Sprintf("%s.json", game.ID)
						fn2 := fmt.Sprintf("%s.playbyplay.json", game.ID)

						BlobPut(BucketRaw(cy), fn, box)
						
						plays := stats.NbaGetPlaysJson(game.ID)

						BlobPut(BucketRaw(cy), fn2, plays)

						ScheduleMap[game.ID] = true

					}
					
				}
	
			}

		}

	}

} // pullAll


func pullBoxscores() {
	
	loadSchedule(cy)

	BlobList(BucketRaw(cy))

	pullAll()

} // pullBoxscores
