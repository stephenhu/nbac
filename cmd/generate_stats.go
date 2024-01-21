package cmd

import (
	"encoding/json"
	"log"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	playersCache 		map[int]stats.PlayerSeason
	//teams 			map[int]map[string]stats.NbaTeamData
)


var (

	generateStatsCmd = &cobra.Command{
		Use: "stats",
		Short: "calculate statistics",
		Long: "calculate season statistics",
		Run: func(cmd *cobra.Command, args []string) {
			generateStats()
		},
		
	}

)


func init() {
} // init


func addPlayerStats(g stats.NbaGame, home bool) {

	teamPlayers := g.Home.Players

	if !home {
		teamPlayers = g.Away.Players
	}

	for _, p := range teamPlayers {

		player := playersCache[p.ID]

		player.Info = stats.TPlayerInfo(p)

		if player.Games == nil {
			player.Games = make(map[string]stats.PlayerGame)
		}

		pg := stats.TPlayerGame(p)

		pg.HomeCode			= g.Home.ShortName
		pg.AwayCode			= g.Away.ShortName
		pg.GameDate     = g.GameTime

		player.Games[g.ID] = pg

		playersCache[p.ID] = player

	}

} // addPlayerStats


func initStructures() {
	playersCache = make(map[int]stats.PlayerSeason)
} // initStructures


func generatePlayerStats() {

	j, err := json.MarshalIndent(playersCache, STR_EMPTY, STR_INDENT)

	if err != nil {
		log.Println(err)
	} else {
		write(j, "players.json")
	}

} // generatePlayerStats


func generateStats() {

	initWarehouseDir()

	initStructures()

	//scores := parseBoxscores()

	//generatePlayerStats()

} // generateStats
