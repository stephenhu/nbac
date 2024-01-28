package cmd

import (
	"fmt"
	//"log"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	pullNbaCmd = &cobra.Command{
		Use: "nba",
		Short: "nba statistics",
		Long: "nba statistics from the official NBA APIs",
		Run: func(cmd *cobra.Command, args []string) {
			pullResume()
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


func getSchedule() *stats.NbaSchedule {

	s := stats.NbaSchedule{}

	fn := fmt.Sprintf("%s/schedule.json", fDir)

	if !fileExists(fn) {

		buf := stats.NbaGetScheduleJson()

		write(buf, fn)

	} else {

		read(&s, fn)

	}

	schedule := stats.NbaSchedule{}

	read(&schedule, fn)

	return &schedule

} // getSchedule


func pullFrom(d string) {

	schedule := getSchedule()

	for _, day := range schedule.LeagueSchedule.GameDates {

		if !stats.IsFutureGame(day.GameDate) {

			gd := stats.GameDateToString(day.GameDate)

			if gd > d {

				for _, game := range day.Games {
					
					if game.WeekNumber > 0 {
						
						box := stats.NbaGetBoxscoreJson(game.ID)
	
							name := stats.GameDateToString(day.GameDate)
	
							dir := fmt.Sprintf("%s/%s", fDir, name)
	
							if !fileExists(dir) {
								createDir(dir)
							}
	
							fn 	:= fmt.Sprintf("%s/%s.json", dir, game.ID)
							fn2 := fmt.Sprintf("%s/%s_playbyplay.json", dir, game.ID)

							if !fileExists(fn) {
								write(box, fn)
							}

							plays := stats.NbaGetPlaysJson(game.ID)

							if !fileExists(fn2) {
								write(plays, fn2)
							}
			
						//}
						
					}
	
				}

			}

		}

	}

} // pullFrom


func pullResume() {

	initDir()
	
	days := getGameDays()

	if len(days) == 1 || len(days) == 0 {
		pullFrom(FROM_SEASON_BEGIN)
	} else {

		lastDay := getLastDay(days)

		pullFrom(lastDay)

	}

} // pullResume
