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


func getSchedule() *stats.NbaSchedule {

	var schedule = stats.NbaSchedule{}

	fn := fmt.Sprintf("%s/schedule.json", fDir)
 
	if !fileExists(fn) {

		schedule = *stats.NbaGetSchedule()

		writeJson(schedule, fn)

	} else {
		readJson(&schedule, fn)
	}

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
						
						box := stats.NbaGetBoxscore(game.ID)
	
						if len(box.Meta.Time) != 0 && len(box.Meta.Request) != 0 {
	
							name := stats.GameDateToString(day.GameDate)
	
							// TODO: is this windows friendly?
							dir := fmt.Sprintf("%s/%s", fDir, name)
	
							if !fileExists(dir) {
								createDir(dir)
							}
	
							fn := fmt.Sprintf("%s/%s.json", dir, game.ID)

							if !fileExists(fn) {
								writeJson(box, fn)
							}
			
						}
						
					}
	
				}

			}

		}

	}

} // pullFrom


func pullResume() {

	days := getGameDays()

	if len(days) == 1 || len(days) == 0 {
		pullFrom(FROM_SEASON_BEGIN)
	} else {

		// this will fail if there other directories with non date names
		pullFrom(days[len(days)-2].Name())

	}

} // pullResume
