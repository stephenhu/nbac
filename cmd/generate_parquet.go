package cmd

import (
	"fmt"
  "log"
	"os"
	//"path/filepath"
	//"strings"
	"time"

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
	leaders       map[int]*stats.Base
)


var (

	generateParquetCmd = &cobra.Command{
		Use: "parquet",
		Short: "calculate statistics and store to parquet",
		Long: "calculate season statistics",
		Run: func(cmd *cobra.Command, args []string) {
			generateParquet()
		},
		
	}

)


func init() {
} // init


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
		{Name: "plusMinus", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
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
	}, nil)

} // createLeadersSchema


func createStandingsSchema() *arrow.Schema {

	return arrow.NewSchema([]arrow.Field{
		{Name: "playerId", Type: arrow.BinaryTypes.String, Nullable: false},
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
		{Name: "plusMinus", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "positon", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "minutes", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "fastbreak", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "paint", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "secondChance", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "gameType", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "games", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

} // createStandingsSchema


func playedGame(mins int) int {

	if mins > 0 {
		return 1
	} else {
		return 0
	}

} // playedGame


func percentage(attempted int, made int) float32 {

	if attempted == 0 {
		return 0.0
	} else {
		return float32(made)/float32(attempted)
	}

} // percentage


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
		rb.Field(37).(*array.Int32Builder).Append(int32(p.Statistics.PlusMinus))
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

		if ok {

			player.Points     += p.Statistics.Points
			player.Oreb       += p.Statistics.Oreb
			player.Dreb       += p.Statistics.Dreb
			player.Treb       += p.Statistics.Treb 
			player.Assists    += p.Statistics.Assists
			player.Steals     += p.Statistics.Steals
			player.Turnovers  += p.Statistics.Turnovers
			player.Blocks     += p.Statistics.Blocks
			player.Blocked    += p.Statistics.Blocked
			player.Fouls      += p.Statistics.Fouls
			player.FoulsO   	+= p.Statistics.FoulsOff
			player.Technicals += p.Statistics.Technicals
			player.Fouled += p.Statistics.FoulsDrawn
			player.Fta        += p.Statistics.Fta
			player.Ftm        += p.Statistics.Ftm
			player.Fgta       += p.Statistics.Fga
			player.Fgtm       += p.Statistics.Fgm
			player.Fg2a       += p.Statistics.Fg2a
			player.Fg2m       += p.Statistics.Fg2m
			player.Fg3a       += p.Statistics.Fg3a
			player.Fg3m       += p.Statistics.Fg3m
			player.PlusMinus  += p.Statistics.PlusMinus
			player.Minutes    += mins
			player.Fastbreak += p.Statistics.PointsFast
			player.Paint 		+= p.Statistics.PointsPaint
			player.SecondChance		+= p.Statistics.PointsSecond
			player.Games 					+= pg
			
		} else {

			np := stats.Base{
				ID:									p.ID,
				First:							p.First,
				Last:								p.Last,
				Full:       				p.Name,
				Abv:  							p.NameShort,
				Points: 						p.Statistics.Points,
				Oreb: 							p.Statistics.Oreb,
				Dreb: 							p.Statistics.Dreb,
				Treb: 							p.Statistics.Treb,
				Assists: 						p.Statistics.Assists,
				Steals: 						p.Statistics.Steals,
				Turnovers: 					p.Statistics.Turnovers,
				Blocks: 						p.Statistics.Blocks,
				Blocked: 						p.Statistics.Blocked,
				Fouls: 							p.Statistics.Fouls,
				FoulsO: 						p.Statistics.FoulsOff,
				Technicals: 				p.Statistics.Technicals,
				Fouled: 						p.Statistics.FoulsDrawn,
				Fta: 								p.Statistics.Fta,
				Ftm: 								p.Statistics.Ftm,
				Fgta: 							p.Statistics.Fga,
				Fgtm: 							p.Statistics.Fgm,
				Fg2a: 							p.Statistics.Fg2a,
				Fg2m: 							p.Statistics.Fg2m,
				Fg3a: 							p.Statistics.Fg3a,
				Fg3m: 							p.Statistics.Fg3m,
				PlusMinus: 					p.Statistics.PlusMinus,
				Minutes: 						mins,
				Fastbreak: 					p.Statistics.PointsFast,
				Paint: 							p.Statistics.PointsPaint,
				SecondChance:				p.Statistics.PointsSecond,
				Games:              pg,
			}

			leaders[p.ID] = &np

		}

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
		rb.Field(6).(*array.Int32Builder).Append(int32(p.Points))
		rb.Field(7).(*array.Int32Builder).Append(int32(p.Oreb))
		rb.Field(8).(*array.Int32Builder).Append(int32(p.Dreb))
		rb.Field(9).(*array.Int32Builder).Append(int32(p.Treb))
		rb.Field(10).(*array.Int32Builder).Append(int32(p.Assists))
		rb.Field(11).(*array.Int32Builder).Append(int32(p.Steals))
		rb.Field(12).(*array.Int32Builder).Append(int32(p.Turnovers))
		rb.Field(13).(*array.Int32Builder).Append(int32(p.Blocks))
		rb.Field(14).(*array.Int32Builder).Append(int32(p.Blocked))
		rb.Field(15).(*array.Int32Builder).Append(int32(p.Fouls))
		rb.Field(16).(*array.Int32Builder).Append(int32(p.FoulsO))
		rb.Field(17).(*array.Int32Builder).Append(int32(p.Technicals))
		rb.Field(18).(*array.Int32Builder).Append(int32(p.Fouled))
		rb.Field(19).(*array.Int32Builder).Append(int32(p.Fta))
		rb.Field(20).(*array.Int32Builder).Append(int32(p.Ftm))
		rb.Field(21).(*array.Float32Builder).Append(float32(
			percentage(p.Fta, p.Ftm)))
		rb.Field(22).(*array.Int32Builder).Append(int32(p.Fg2a))
		rb.Field(23).(*array.Int32Builder).Append(int32(p.Fg2m))
		rb.Field(24).(*array.Float32Builder).Append(float32(
			percentage(p.Fg2a, p.Fg2m)))
		rb.Field(25).(*array.Int32Builder).Append(int32(p.Fg3a))
		rb.Field(26).(*array.Int32Builder).Append(int32(p.Fg3m))
		rb.Field(27).(*array.Float32Builder).Append(float32(
			percentage(p.Fg3a, p.Fg3m)))
		rb.Field(28).(*array.Int32Builder).Append(int32(p.Fgta))
		rb.Field(29).(*array.Int32Builder).Append(int32(p.Fgtm))
		rb.Field(30).(*array.Float32Builder).Append(float32(
			percentage(p.Fgta, p.Fgtm)))
		rb.Field(31).(*array.Float32Builder).Append(float32(p.PlusMinus))
		rb.Field(32).(*array.Int32Builder).Append(int32(p.Minutes))
		rb.Field(33).(*array.Int32Builder).Append(int32(p.Fastbreak))
		rb.Field(34).(*array.Int32Builder).Append(int32(p.Paint))
		rb.Field(35).(*array.Int32Builder).Append(int32(p.SecondChance))
		rb.Field(36).(*array.Int32Builder).Append(int32(p.Games))

	}

} // leaderParquet


func getNowStamp() string {
  
	now := time.Now()
	
	return now.Format(NBAC_DATE_FORMAT)

} // getNowStamp


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

		boxscoreAggregator(score.Game.Home.Players)
		boxscoreAggregator(score.Game.Away.Players)
	
	}

	leaderParquet(builder)

	flushParquet(leadersSchema, builder, LEADERS_PREFIX)

} // generateLeaders


func generateParquet() {

	initWarehouseDir()

	schedule 	= getSchedule()

	scores 		= parseBoxscores()

	leaders = make(map[int]*stats.Base)

	gt = stats.TGameType(schedule.LeagueSchedule.GameDates)

	generateGames()

	generatePlayers()

	generateLeaders()

} // generateParquet
