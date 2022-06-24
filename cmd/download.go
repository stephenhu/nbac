package cmd

import (
	//"fmt"
	//"io"
	"log"
	//"net/http"
	//"net/url"
	//"os"
	//"sort"
	//"strings"

	//"github.com/PuerkitoBio/goquery"
	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	fPattern				string
	fFile						string
	fExclude        []string

	 downloadCmd = &cobra.Command{
		Use: "download",
		Short: "Download statistics",
		Long: "Download statistics from the NBA's APIs",
		Run: func(cmd *cobra.Command, args []string) {

			download()

		},
	}

)


func download() {

	start := stats.YEAR_MODERN_ERA

	years := stats.GetYearsFrom(stats.YEAR_MODERN_ERA)

	log.Println(years)

	/*
	days := stats.GetDaysByYear("2019")

	for _, d := range days {

		log.Println(d)

		s := stats.NbaGetScoreboard(d)

		g := stats.NbaGetBoxscores(s)

		log.Println(g)

	}
	*/

} // download
