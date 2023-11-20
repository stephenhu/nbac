package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	pullNbaCmd = &cobra.Command{
		Use: "nba",
		Short: "nba statistics",
		Long: "nba statistics from the official NBA APIs",
		Run: func(cmd *cobra.Command, args []string) {
			pullAll()
		},
	}

)


func init() {



} // init


func getSchedule() *stats.NbaSchedule {

	schedule := stats.NbaGetSchedule()

	fn := fmt.Sprintf("%s/schedule.json", fDir)

	writeJson(schedule, fn)

	return schedule

} // getSchedule


func pullAll() {

	schedule := getSchedule()

	for _, day := range schedule.LeagueSchedule.GameDates {

		if !stats.IsFutureGame(day.GameDate) {

			for _, game := range day.Games {
				if game.WeekNumber > 0 {
					
					box := stats.NbaGetBoxscore(game.ID)

					if len(box.Meta.Time) != 0 && len(box.Meta.Request) != 0 {

						name := stats.UtcToFolder(box.Game.GameTime)

						// TODO: is this windows friendly?
						dir := fmt.Sprintf("%s/%s", fDir, name)

						if !dirExists(dir) {
							createDir(dir)
						}

						fn := fmt.Sprintf("%s/%s.json", dir, game.ID)

						writeJson(box, fn)
		
					}
					
				}

			}

		}

	}

} // pullAll
