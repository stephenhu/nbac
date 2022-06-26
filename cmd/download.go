package cmd

import (
	"encoding/json"
	"fmt"
	//"io"
	"log"
	//"net/http"
	//"net/url"
	"os"
	//"sort"
	"strings"

	//"github.com/PuerkitoBio/goquery"
	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	fPattern				string
	fFile						string

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

	years := stats.GetYearsFrom(stats.YEAR_MODERN_ERA)

	for _, y := range years {

		days := stats.GetDaysBySeason(y)

		dirName := fmt.Sprintf("%d", y)

		err := os.Mkdir(dirName, 0755)

		if err != nil {
			log.Println(err)
		} else {

			for _, d := range days {

				s := stats.NbaGetScoreboard(d)

				b := stats.NbaGetBoxscores(s)

				err := os.Mkdir(fmt.Sprintf("%s/%s", dirName, d), 0755)

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

			}
	
		}

	}

} // download
