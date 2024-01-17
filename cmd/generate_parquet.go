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
	NBAC_DATE_FORMAT		= "20060102"
	PARQUET_EXT					= ".parquet"
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


func boxscoreToParquet() {

} // boxscoreToParquet


func createPlayerSchema() *arrow.Schema {

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "gameTime", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "playerId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "teamId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "teamShort", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "first", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "full", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "abv", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "gameId", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "opponentId", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "opponentShort", Type: arrow.BinaryTypes.String, Nullable: false},
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
	}, nil)

	return schema

} // createPlayerSchema


func addRecordToParquet(s stats.NbaBoxscore, home bool,
	rb *array.RecordBuilder) {

	players := s.Game.Home.Players

	if !home {
		players = s.Game.Away.Players
	}

	for _, p := range players {

		rb.Field(0).(*array.StringBuilder).Append(s.Game.GameTime)
		rb.Field(1).(*array.Int32Builder).Append(int32(p.ID))
		rb.Field(2).(*array.Int32Builder).Append(0)
		rb.Field(3).(*array.StringBuilder).Append("")
		rb.Field(4).(*array.StringBuilder).Append(p.First)
		rb.Field(5).(*array.StringBuilder).Append(p.Last)
		rb.Field(6).(*array.StringBuilder).Append(p.Name)
		rb.Field(7).(*array.StringBuilder).Append(p.NameShort)
		rb.Field(8).(*array.StringBuilder).Append(s.Game.ID)
		rb.Field(9).(*array.Int32Builder).Append(0)
		rb.Field(10).(*array.StringBuilder).Append("")
		rb.Field(11).(*array.Int32Builder).Append(int32(p.Statistics.Points))
		rb.Field(12).(*array.Int32Builder).Append(int32(p.Statistics.Oreb))
		rb.Field(13).(*array.Int32Builder).Append(int32(p.Statistics.Dreb))
		rb.Field(14).(*array.Int32Builder).Append(int32(p.Statistics.Treb))
		rb.Field(15).(*array.Int32Builder).Append(int32(p.Statistics.Assists))
		rb.Field(16).(*array.Int32Builder).Append(int32(p.Statistics.Steals))
		rb.Field(17).(*array.Int32Builder).Append(int32(p.Statistics.Turnovers))
		rb.Field(18).(*array.Int32Builder).Append(int32(p.Statistics.Blocks))
		rb.Field(19).(*array.Int32Builder).Append(int32(p.Statistics.Blocked))
		rb.Field(20).(*array.Int32Builder).Append(int32(p.Statistics.Fouls))
		rb.Field(21).(*array.Int32Builder).Append(int32(p.Statistics.FoulsOff))
		rb.Field(22).(*array.Int32Builder).Append(int32(p.Statistics.Technicals))
		rb.Field(23).(*array.Int32Builder).Append(int32(p.Statistics.FoulsDrawn))
		rb.Field(24).(*array.Int32Builder).Append(int32(p.Statistics.Fta))
		rb.Field(25).(*array.Int32Builder).Append(int32(p.Statistics.Ftm))
		rb.Field(26).(*array.Float32Builder).Append(float32(0.0))
		rb.Field(27).(*array.Int32Builder).Append(int32(p.Statistics.Fg2a))
		rb.Field(28).(*array.Int32Builder).Append(int32(p.Statistics.Fg2m))
		rb.Field(29).(*array.Float32Builder).Append(float32(0.0))
		rb.Field(30).(*array.Int32Builder).Append(int32(p.Statistics.Fg3a))
		rb.Field(31).(*array.Int32Builder).Append(int32(p.Statistics.Fg3m))
		rb.Field(32).(*array.Float32Builder).Append(float32(0.0))
		rb.Field(33).(*array.Int32Builder).Append(int32(p.Statistics.Fga))
		rb.Field(34).(*array.Int32Builder).Append(int32(p.Statistics.Fgm))
		rb.Field(35).(*array.Float32Builder).Append(float32(0.0))
		rb.Field(36).(*array.Int32Builder).Append(int32(p.Statistics.PlusMinus))
		rb.Field(37).(*array.StringBuilder).Append(p.Position)
		rb.Field(38).(*array.Int32Builder).Append(int32(stats.PtmToMin(p.Statistics.Minutes)))
		rb.Field(39).(*array.Int32Builder).Append(int32(p.Statistics.PointsFast))
		rb.Field(40).(*array.Int32Builder).Append(int32(p.Statistics.PointsPaint))
		rb.Field(41).(*array.Int32Builder).Append(int32(p.Statistics.PointsSecond))

	}

} // addRecordToParquet


func getNowStamp() string {
  
	now := time.Now()
	
	return now.Format(NBAC_DATE_FORMAT)

} // getNowStamp


func flushParquet(schema *arrow.Schema, b *array.RecordBuilder) {

	pf := fmt.Sprintf("%s/%s/%s%s", fDir, WAREHOUSE_DIR, getNowStamp(),
	  PARQUET_EXT)

	rec := b.NewRecord()
	defer rec.Release()
	
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	f, err := os.Create(pf)

	if err != nil {
		log.Println(err)
	} else {

		err := pqarrow.WriteTable(tbl, f, 1024, nil, pqarrow.DefaultWriterProps())

		if err != nil {
			log.Println(err)
		}

	}

} // flushParquet


func generateParquet() {

	initWarehouseDir()

	playerSchema := createPlayerSchema()

	scores := parseBoxscores()

	builder := array.NewRecordBuilder(memory.DefaultAllocator,
		playerSchema)
	defer builder.Release()

	for _, score := range scores {

		addRecordToParquet(score, true, builder)
		addRecordToParquet(score, false, builder)
	
	}

	flushParquet(playerSchema, builder)

} // generateParquet
