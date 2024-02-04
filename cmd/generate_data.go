package cmd

import (
	"encoding/json"
	"fmt"
  "log"
	"os"

	"github.com/apache/arrow/go/v15/arrow"
  "github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"

)


const (
	NBAC_DATE_FORMAT				= "20060102"
	PARQUET_EXT							= ".parquet"
	PLAYERS_PREFIX      		= "players"
	GAMES_PREFIX      			= "games"
	LEADERS_PREFIX      		= "leaders"
	STANDINGS_PREFIX    		= "standings"
)


var (
  gt 						map[string]int
	schedule			*stats.NbaSchedule
	scores        []stats.NbaBoxscore
	leaders       map[int]*stats.Leaders
	standings			map[int]*stats.Standing
)


var (

	generateDataCmd = &cobra.Command{
		Use: "data",
		Short: "calculate statistics and store to parquet",
		Long: "calculate season statistics",
		Run: func(cmd *cobra.Command, args []string) {
			generateData()
		},
		
	}

)


func init() {
} // init


func checkWin(game *stats.NbaGame) bool {

	if game.Home.Statistics.Points > game.Away.Statistics.Points {
		return true
	} else {
		return false
	}
	
} // checkWin


func createGamesSchema() *arrow.Schema {

	return arrow.NewSchema([]arrow.Field{
		{Name: "leagueId", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "seasonYear", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "gameTime", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "weekNumber", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "homeId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "homeCode", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "awayId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "awayCode", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "gameId", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

} // createGamesSchema


func createPlayerSchema() *arrow.Schema {

	return arrow.NewSchema([]arrow.Field{
		{Name: "gameTime", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "playerId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "teamId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "homeId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "homeShort", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "awayId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "awayShort", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "first", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "full", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "abv", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "gameId", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "points", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "oreb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "dreb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "treb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "assists", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "steals", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "turnovers", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "blocks", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "blocked", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fouls", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "foulsOffensive", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "technicals", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fouled", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fta", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ftm", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ftp", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg2a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg2m", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg2p", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg3a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg3m", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg3p", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fgta", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fgtm", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fgtp", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "plusMinus", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "position", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "minutes", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fastbreak", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "paint", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "secondChance", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "gameType", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

} // createPlayerSchema


func createLeadersSchema() *arrow.Schema {

	return arrow.NewSchema([]arrow.Field{
		{Name: "playerId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "seasonYear", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "first", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "full", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "abv", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "points", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "oreb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "dreb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "treb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "assists", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "steals", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "turnovers", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "blocks", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "blocked", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fouls", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "foulsOffensive", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "technicals", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fouled", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fta", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ftm", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ftp", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg2a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg2m", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg2p", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg3a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg3m", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg3p", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fgta", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fgtm", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fgtp", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "plusMinus", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "minutes", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fastbreak", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "paint", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "secondChance", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "games", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ppg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "orebPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "drebPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "trebPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "assistsPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "stealsPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "turnoversPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "blocksPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "blockedPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "foulsPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "foulsOffensivePg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "technicalsPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fouledPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "ftaPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "ftmPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg2aPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg2mPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg3aPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg3mPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fgtaPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fgtmPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fastbreakPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "paintPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "secondChancePg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "plusMinusPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "minutesPg", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
	}, nil)

} // createLeadersSchema


func createStandingsSchema() *arrow.Schema {

	return arrow.NewSchema([]arrow.Field{
		{Name: "teamId", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "code", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "rank", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "win", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "loss", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "wpct", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "gb", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "homeW", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "homeL", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "awayW", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "awayL", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "divW", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "divL", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "confW", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "confL", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "lastTenW", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "lastTenL", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "streakW", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "streakL", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "totalPf", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "totalPa", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "p1", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "p2", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "p3", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "p4", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "pot", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "oreb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "dreb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "treb", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "assists", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "at", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "bench", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "biggestLead", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "biggestRun", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "pointsFromTurnovers", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "steals", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "turnovers", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "blocks", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "blocked", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fouls", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "foulsOffensive", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "technicals", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fouled", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fta", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ftm", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ftp", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg2a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg2m", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg2p", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fg3a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg3m", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fg3p", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "fgta", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fgtm", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fgtp", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "efg", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "trueA", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "trueM", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "trueP", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "leadTime", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ties", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fastbreak", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "paint", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "secondChance", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "gameType", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

} // createStandingsSchema


func boxscoreToParquet(s stats.NbaBoxscore, home bool,
	rb *array.RecordBuilder) {

	players := s.Game.Home.Players

	if !home {
		players = s.Game.Away.Players
	}

	for _, p := range players {

		rb.Field(0).(*array.StringBuilder).Append(s.Game.GameTime)
		rb.Field(1).(*array.Int32Builder).Append(int32(p.ID))

		if home {
			rb.Field(2).(*array.Int32Builder).Append(int32(s.Game.Home.ID))
		} else {
			rb.Field(2).(*array.Int32Builder).Append(int32(s.Game.Away.ID))
		}
		
		rb.Field(3).(*array.Int32Builder).Append(int32(s.Game.Home.ID))
		rb.Field(4).(*array.StringBuilder).Append(s.Game.Home.ShortName)
    rb.Field(5).(*array.Int32Builder).Append(int32(s.Game.Away.ID))
	  rb.Field(6).(*array.StringBuilder).Append(s.Game.Away.ShortName)
		rb.Field(7).(*array.StringBuilder).Append(p.First)
		rb.Field(8).(*array.StringBuilder).Append(p.Last)
		rb.Field(9).(*array.StringBuilder).Append(p.Name)
		rb.Field(10).(*array.StringBuilder).Append(p.NameShort)
		rb.Field(11).(*array.StringBuilder).Append(s.Game.ID)
		rb.Field(12).(*array.Int32Builder).Append(int32(p.Statistics.Points))
		rb.Field(13).(*array.Int32Builder).Append(int32(p.Statistics.Oreb))
		rb.Field(14).(*array.Int32Builder).Append(int32(p.Statistics.Dreb))
		rb.Field(15).(*array.Int32Builder).Append(int32(p.Statistics.Treb))
		rb.Field(16).(*array.Int32Builder).Append(int32(p.Statistics.Assists))
		rb.Field(17).(*array.Int32Builder).Append(int32(p.Statistics.Steals))
		rb.Field(18).(*array.Int32Builder).Append(int32(p.Statistics.Turnovers))
		rb.Field(19).(*array.Int32Builder).Append(int32(p.Statistics.Blocks))
		rb.Field(20).(*array.Int32Builder).Append(int32(p.Statistics.Blocked))
		rb.Field(21).(*array.Int32Builder).Append(int32(p.Statistics.Fouls))
		rb.Field(22).(*array.Int32Builder).Append(int32(p.Statistics.FoulsOff))
		rb.Field(23).(*array.Int32Builder).Append(int32(p.Statistics.Technicals))
		rb.Field(24).(*array.Int32Builder).Append(int32(p.Statistics.FoulsDrawn))
		rb.Field(25).(*array.Int32Builder).Append(int32(p.Statistics.Fta))
		rb.Field(26).(*array.Int32Builder).Append(int32(p.Statistics.Ftm))
		rb.Field(27).(*array.Float32Builder).Append(float32(p.Statistics.Ftp))
		rb.Field(28).(*array.Int32Builder).Append(int32(p.Statistics.Fg2a))
		rb.Field(29).(*array.Int32Builder).Append(int32(p.Statistics.Fg2m))
		rb.Field(30).(*array.Float32Builder).Append(float32(p.Statistics.Fg2p))
		rb.Field(31).(*array.Int32Builder).Append(int32(p.Statistics.Fg3a))
		rb.Field(32).(*array.Int32Builder).Append(int32(p.Statistics.Fg3m))
		rb.Field(33).(*array.Float32Builder).Append(float32(p.Statistics.Fg3p))
		rb.Field(34).(*array.Int32Builder).Append(int32(p.Statistics.Fga))
		rb.Field(35).(*array.Int32Builder).Append(int32(p.Statistics.Fgm))
		rb.Field(36).(*array.Float32Builder).Append(float32(p.Statistics.Fgp))
		rb.Field(37).(*array.Float32Builder).Append(float32(p.Statistics.PlusMinus))
		rb.Field(38).(*array.StringBuilder).Append(p.Position)
		rb.Field(39).(*array.Int32Builder).Append(int32(stats.PtmToMin(p.Statistics.Minutes)))
		rb.Field(40).(*array.Int32Builder).Append(int32(p.Statistics.PointsFast))
		rb.Field(41).(*array.Int32Builder).Append(int32(p.Statistics.PointsPaint))
		rb.Field(42).(*array.Int32Builder).Append(int32(p.Statistics.PointsSecond))
		rb.Field(43).(*array.Int32Builder).Append(int32(gt[s.Game.ID]))

	}

} // boxscoreToParquet


func boxscoreAggregator(players []stats.NbaPlayer) {

	for _, p := range players {

		player, ok := leaders[p.ID]

		mins := stats.PtmToMin(p.Statistics.Minutes)

		pg := playedGame(mins)

		if !ok {
			player = &stats.Leaders{}
		}

		player.ID				= p.ID
		player.First    = p.First
		player.Last    	= p.Last
		player.Full   	= p.Name
		player.Abv    	= p.NameShort
		
		player.Base.Points     += p.Statistics.Points
		player.Base.Oreb       += p.Statistics.Oreb
		player.Base.Dreb       += p.Statistics.Dreb
		player.Base.Treb       += p.Statistics.Treb 
		player.Base.Assists    += p.Statistics.Assists
		player.Base.Steals     += p.Statistics.Steals
		player.Base.Turnovers  += p.Statistics.Turnovers
		player.Base.Blocks     += p.Statistics.Blocks
		player.Base.Blocked    += p.Statistics.Blocked
		player.Base.Fouls      += p.Statistics.Fouls
		player.Base.FoulsO   	+= p.Statistics.FoulsOff
		player.Base.Technicals += p.Statistics.Technicals
		player.Base.Fouled += p.Statistics.FoulsDrawn
		player.Base.Fta        += p.Statistics.Fta
		player.Base.Ftm        += p.Statistics.Ftm
		player.Base.Fgta       += p.Statistics.Fga
		player.Base.Fgtm       += p.Statistics.Fgm
		player.Base.Fg2a       += p.Statistics.Fg2a
		player.Base.Fg2m       += p.Statistics.Fg2m
		player.Base.Fg3a       += p.Statistics.Fg3a
		player.Base.Fg3m       += p.Statistics.Fg3m
		player.PlusMinus  += p.Statistics.PlusMinus
		player.Minutes    += mins
		player.Base.Fastbreak += p.Statistics.PointsFast
		player.Base.Paint 		+= p.Statistics.PointsPaint
		player.Base.SecondChance		+= p.Statistics.PointsSecond
		player.Games 					+= pg

		player.Advanced.PointsPg     = percentage(p.Statistics.Points, pg)
		player.Advanced.OrebPg       = percentage(p.Statistics.Oreb, pg)
		player.Advanced.DrebPg       = percentage(p.Statistics.Dreb, pg)
		player.Advanced.TrebPg       = percentage(p.Statistics.Treb, pg)
		player.Advanced.AssistsPg    = percentage(p.Statistics.Assists, pg)
		player.Advanced.StealsPg     = percentage(p.Statistics.Steals, pg)
		player.Advanced.TurnoversPg  = percentage(p.Statistics.Turnovers, pg)
		player.Advanced.BlocksPg     = percentage(p.Statistics.Blocks, pg)
		player.Advanced.BlockedPg    = percentage(p.Statistics.Blocked, pg)
		player.Advanced.FoulsPg      = percentage(p.Statistics.Fouls, pg)
		player.Advanced.FoulsOPg   	= percentage(p.Statistics.FoulsOff, pg)
		player.Advanced.TechnicalsPg = percentage(p.Statistics.Technicals, pg)
		player.Advanced.FouledPg = percentage(p.Statistics.FoulsDrawn, pg)
		player.Advanced.FtaPg        = percentage(p.Statistics.Fta, pg)
		player.Advanced.FtmPg        = percentage(p.Statistics.Ftm, pg)
		player.Advanced.FgtaPg       = percentage(p.Statistics.Fga, pg)
		player.Advanced.FgtmPg       = percentage(p.Statistics.Fgm, pg)
		player.Advanced.Fg2aPg       = percentage(p.Statistics.Fg2a, pg)
		player.Advanced.Fg2mPg       = percentage(p.Statistics.Fg2m, pg)
		player.Advanced.Fg3aPg       = percentage(p.Statistics.Fg3a, pg)
		player.Advanced.Fg3mPg       = percentage(p.Statistics.Fg3m, pg)
		player.Advanced.FastbreakPg = percentage(p.Statistics.PointsFast, pg)
		player.Advanced.PaintPg 		= percentage(p.Statistics.PointsPaint, pg)
		player.Advanced.SecondChancePg		= percentage(p.Statistics.PointsSecond, pg)

		leaders[p.ID] = player

	}

} // boxscoreAggregator


func leaderParquet(rb *array.RecordBuilder) {

	for _, p := range leaders {

		rb.Field(0).(*array.Int32Builder).Append(int32(p.ID))
		rb.Field(1).(*array.StringBuilder).Append("")
		rb.Field(2).(*array.StringBuilder).Append(p.First)
		rb.Field(3).(*array.StringBuilder).Append(p.Last)
    rb.Field(4).(*array.StringBuilder).Append(p.Full)
	  rb.Field(5).(*array.StringBuilder).Append(p.Abv)
		rb.Field(6).(*array.Int32Builder).Append(int32(p.Base.Points))
		rb.Field(7).(*array.Int32Builder).Append(int32(p.Base.Oreb))
		rb.Field(8).(*array.Int32Builder).Append(int32(p.Base.Dreb))
		rb.Field(9).(*array.Int32Builder).Append(int32(p.Base.Treb))
		rb.Field(10).(*array.Int32Builder).Append(int32(p.Base.Assists))
		rb.Field(11).(*array.Int32Builder).Append(int32(p.Base.Steals))
		rb.Field(12).(*array.Int32Builder).Append(int32(p.Base.Turnovers))
		rb.Field(13).(*array.Int32Builder).Append(int32(p.Base.Blocks))
		rb.Field(14).(*array.Int32Builder).Append(int32(p.Base.Blocked))
		rb.Field(15).(*array.Int32Builder).Append(int32(p.Base.Fouls))
		rb.Field(16).(*array.Int32Builder).Append(int32(p.Base.FoulsO))
		rb.Field(17).(*array.Int32Builder).Append(int32(p.Base.Technicals))
		rb.Field(18).(*array.Int32Builder).Append(int32(p.Base.Fouled))
		rb.Field(19).(*array.Int32Builder).Append(int32(p.Base.Fta))
		rb.Field(20).(*array.Int32Builder).Append(int32(p.Base.Ftm))
		rb.Field(21).(*array.Float32Builder).Append(float32(
			percentage(p.Base.Fta, p.Base.Ftm)))
		rb.Field(22).(*array.Int32Builder).Append(int32(p.Base.Fg2a))
		rb.Field(23).(*array.Int32Builder).Append(int32(p.Base.Fg2m))
		rb.Field(24).(*array.Float32Builder).Append(float32(
			percentage(p.Base.Fg2a, p.Base.Fg2m)))
		rb.Field(25).(*array.Int32Builder).Append(int32(p.Base.Fg3a))
		rb.Field(26).(*array.Int32Builder).Append(int32(p.Base.Fg3m))
		rb.Field(27).(*array.Float32Builder).Append(float32(
			percentage(p.Base.Fg3a, p.Base.Fg3m)))
		rb.Field(28).(*array.Int32Builder).Append(int32(p.Base.Fgta))
		rb.Field(29).(*array.Int32Builder).Append(int32(p.Base.Fgtm))
		rb.Field(30).(*array.Float32Builder).Append(float32(
			percentage(p.Base.Fgta, p.Base.Fgtm)))
		rb.Field(31).(*array.Float32Builder).Append(float32(p.PlusMinus))
		rb.Field(32).(*array.Int32Builder).Append(int32(p.Minutes))
		rb.Field(33).(*array.Int32Builder).Append(int32(p.Base.Fastbreak))
		rb.Field(34).(*array.Int32Builder).Append(int32(p.Base.Paint))
		rb.Field(35).(*array.Int32Builder).Append(int32(p.Base.SecondChance))
		rb.Field(36).(*array.Int32Builder).Append(int32(p.Games))
		rb.Field(37).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Points)))
		rb.Field(38).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Oreb)))
		rb.Field(39).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Dreb, )))
		rb.Field(40).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Treb)))
		rb.Field(41).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Assists)))
		rb.Field(42).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Steals)))
		rb.Field(43).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Turnovers)))
		rb.Field(44).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Blocks)))
		rb.Field(45).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Blocked)))
		rb.Field(46).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fouls)))
		rb.Field(47).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.FoulsO)))		
		rb.Field(48).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Technicals)))
		rb.Field(49).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fouled)))
		rb.Field(50).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fta)))
		rb.Field(51).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Ftm)))
		rb.Field(52).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fg2a)))
		rb.Field(53).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fg2m)))
		rb.Field(54).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fg3a)))
		rb.Field(55).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fg3m)))
		rb.Field(56).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fgta)))
		rb.Field(57).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fgtm)))
		rb.Field(58).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Fastbreak)))
		rb.Field(59).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.Paint)))
		rb.Field(60).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Base.SecondChance)))
		rb.Field(61).(*array.Float32Builder).Append(float32(
			perGamePercentageFp(p.Games, p.PlusMinus)))
		rb.Field(62).(*array.Float32Builder).Append(float32(
			perGamePercentage(p.Games, p.Minutes)))
	}

} // leaderParquet


func teamAggregator(game *stats.NbaGame, isHome bool) {

	var team stats.NbaTeamScore

	if isHome {
		team = game.Home
	} else {
		team = game.Away
	}

	_, ok := standings[game.Home.ID]

	if !ok {
		standings[team.ID] = &stats.Standing{}
	}

	standings[team.ID].TeamID 	= team.ID
	//standings[team.ID].Name 		= team.Name
	standings[team.ID].Code 		= team.ShortName
	//standings[team.ID].TeamId = team.City

	standings[team.ID].Points 	= team.ID

} // teamAggregator


func flushParquet(schema *arrow.Schema, b *array.RecordBuilder,
	name string) {

	pf := fmt.Sprintf("%s/%s/%s.%s%s", fDir, WAREHOUSE_DIR, name,
	  getNowStamp(), PARQUET_EXT)

	rec := b.NewRecord()
	defer rec.Release()
	
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	f, err := os.Create(pf)

	if err != nil {
		log.Println(err)
	} else {

		err := pqarrow.WriteTable(tbl, f, 1024, nil,
			pqarrow.DefaultWriterProps())

		if err != nil {
			log.Println(err)
		}

	}

} // flushParquet


func generateGames() {

	gamesSchema := createGamesSchema()

	builder := array.NewRecordBuilder(memory.DefaultAllocator,
	  gamesSchema)
	
	defer builder.Release()

	for _, gd := range schedule.LeagueSchedule.GameDates {

		for _, g := range gd.Games {

			builder.Field(0).(*array.StringBuilder).Append(
				schedule.LeagueSchedule.LeagueId)
			
			builder.Field(1).(*array.StringBuilder).Append(
				schedule.LeagueSchedule.SeasonYear)
			
			builder.Field(2).(*array.StringBuilder).Append(g.GameTime)
			builder.Field(3).(*array.Int32Builder).Append(int32(g.WeekNumber))
			builder.Field(4).(*array.Int32Builder).Append(int32(g.Home.ID))
			builder.Field(5).(*array.StringBuilder).Append(g.Home.ShortName)
			builder.Field(6).(*array.Int32Builder).Append(int32(g.Away.ID))
			builder.Field(7).(*array.StringBuilder).Append(g.Away.ShortName)
			builder.Field(8).(*array.StringBuilder).Append(g.ID)

		}

	}

	flushParquet(gamesSchema, builder, GAMES_PREFIX)

} // generateGames


func generatePlayers() {

	playerSchema := createPlayerSchema()

	builder := array.NewRecordBuilder(memory.DefaultAllocator,
		playerSchema)

	defer builder.Release()

	for _, score := range scores {

		boxscoreToParquet(score, true, builder)
		boxscoreToParquet(score, false, builder)
	
	}

	flushParquet(playerSchema, builder, PLAYERS_PREFIX)

} // generatePlayers


func generateLeaders() {

	leadersSchema := createLeadersSchema()

	builder := array.NewRecordBuilder(memory.DefaultAllocator,
		leadersSchema)

	defer builder.Release()

	for _, score := range scores {

		if gt[score.Game.ID] == stats.REGULAR {
			
			boxscoreAggregator(score.Game.Home.Players)
			boxscoreAggregator(score.Game.Away.Players)

		}
	
	}

	leaderParquet(builder)

	flushParquet(leadersSchema, builder, LEADERS_PREFIX)

} // generateLeaders


func generateStandings() {

	for _, score := range scores {

		stats.ParseWl(standings, &score.Game)

		teamAggregator(&score.Game, true)
		teamAggregator(&score.Game, false)

	}

	fn := fmt.Sprintf("%s/%s/%s.%s%s", fDir, WAREHOUSE_DIR,
    STANDINGS_PREFIX, getNowStamp(), EXT_JSON)
	
	j, err := json.Marshal(standings)

	if err != nil {
		log.Println(err)		
	} else {
		write(j, fn)
	}
	
} // generateStandings


func initScheduleGameTypes() {

	gt = make(map[string]int)

	for _, days := range schedule.LeagueSchedule.GameDates {
		for _, g := range days.Games {
			
			if g.WeekNumber == 0 {
				gt[g.ID] = stats.PRESEASON
			} else if g.WeekNumber > 0 {
				gt[g.ID] = stats.REGULAR
			} else if g.WeekNumber > 25 {
				gt[g.ID] = stats.PLAYOFFS
			}

		}
	}

} // initScheduleGameTypes


func generateData() {

	initWarehouseDir()

	schedule = getSchedule()

	scores = parseBoxscores()

	leaders = make(map[int]*stats.Leaders)

	standings = make(map[int]*stats.Standing)

	initScheduleGameTypes()

	generateGames()

	generatePlayers()

	generateLeaders()

	generateStandings()

	} // generateData
