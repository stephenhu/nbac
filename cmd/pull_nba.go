package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

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


func pullGames(d string, y int) {

	s := stats.NbaGetScoreboard(d)

	b := stats.NbaGetBoxscores(s)

	err := os.Mkdir(fmt.Sprintf("%d/%s", y, d), 0755)

	if err != nil {
		log.Println(err)
	} else {

		for _, game := range b {

			j, err := json.MarshalIndent(game, stats.STRING_EMPTY,
				stats.STRING_TAB)

			if err != nil {
				log.Println(err)
			} else {

				fileName := fmt.Sprintf("%d/%s/%s%s.json", y, d,
					strings.ToLower(game.AwayScore.ShortName),
					strings.ToLower(game.HomeScore.ShortName))

				err := os.WriteFile(fileName, j, 0660)

				if err != nil {
					log.Println(err)
				}

			}

		}

	}

} // pullGames


func pullPlayers(y int) {

	players := stats.NbaGetPlayers(y)

	j, err := json.MarshalIndent(players, stats.STRING_EMPTY,
		stats.STRING_TAB)

	if err != nil {
		log.Println(err)
	} else {

		err := os.WriteFile(fmt.Sprintf("%d/players.json", y), j, 0660)

		if err != nil {
			log.Println(err)
		}
	
	}

} // pullPlayers


func pullTeams(y int) {

	teams := stats.NbaGetTeams(y)

	j, err := json.MarshalIndent(teams, stats.STRING_EMPTY,
		stats.STRING_TAB)

	if err != nil {
		log.Println(err)
	} else {

		err := os.WriteFile(fmt.Sprintf("%d/teams.json", y), j, 0660)

		if err != nil {
			log.Println(err)
		}
	
	}

} // pullTeams


func pullAll() {

	years := stats.GetYearsFrom(fSeason)

	for _, y := range years {

		days := stats.GetDaysBySeason(y)

		dirName := fmt.Sprintf("%d", y)

		err := os.Mkdir(dirName, 0755)

		if err != nil {
			log.Println(err)
		} else {

			pullPlayers(y)
			pullTeams(y)

			for _, d := range days {
				
				pullGames(d, y)

			}

		}
	
	}

} // pullAll
