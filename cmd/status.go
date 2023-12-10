package cmd

import (
	"fmt"
	"log"
	"os"
	"path"
	//"path/filepath"
	//"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (

	fSched		*stats.NbaSchedule

	statusCmd = &cobra.Command{
		Use: "status",
		Short: "status check",
		Long: "status check all data",
		Run: func(cmd *cobra.Command, args []string) {
			getStatus()
		},
	}

)


func init() {

	statusCmd.PersistentFlags().StringVarP(&fDir, "dir", "d", 
	  stats.GetCurrentSeason(), "Directory where data is stored")

	fSched = getSchedule()

} // init


func getTotalGames() int {

	total := 0

	for _, day := range fSched.LeagueSchedule.GameDates {
		total += len(day.Games)
	}

	return total

} // getTotalGames


func getDownloaded() (int, int) {

	total 		:= 0
	ptotal 		:= 0

	dirs, err := os.ReadDir(fDir)

	if err != nil {
		log.Println(err)
	} else {
	
		for _, day := range dirs {

			if day.IsDir() {

				fn := fmt.Sprintf("%s/%s", fDir, day.Name())
				
				games, err := os.ReadDir(fn)

				if err != nil {
					log.Println(err)
				} else {

					for _, g := range games {

						if path.Ext(path.Join(fn, g.Name())) == EXT_JSON {

							if strings.Contains(g.Name(), PBP_SUFFIX) {
								ptotal += 1
							} else {
								total += 1
							}
	
						}

					}

				}
	
			}

		}

	}

	return total, ptotal

} // getDownloaded


func getGameDays() []os.DirEntry {

	days, err := os.ReadDir(fDir)

	if err != nil {
		
		log.Println(err)
		return []os.DirEntry{}

	} else {
		return days
	}

} // getGameDays


func getGameCountByDay(d string) int {

	for _, day := range fSched.LeagueSchedule.GameDates {

		t, err := time.Parse(stats.NBA_DATETIME_FORMAT, day.GameDate)

		if err != nil {
			log.Println(err)
		} else {

			if t.Format(stats.DATE_FORMAT) == d {
				return len(day.Games) * 2
			}
	
		}

	}

	return 0

} // getGameCountByDay


func getGameDayDetail() {

	fmt.Println("\n\nDetailed Downloads by Day:\n")

	days := getGameDays()

	for _, day := range days {

		if day.IsDir() {

			fn := fmt.Sprintf("%s/%s", fDir, day.Name())

			files, err := os.ReadDir(fn)
		
			if err != nil {
				log.Println(err)
			} else {
	
				fmt.Printf("%s:\t (%d/%d)\n", day.Name(), len(files),
				  getGameCountByDay(day.Name()))
		
			}
	
		}
	
	}

} // getGameDayDetail


func getEstTimeNow() time.Time {

	now := time.Now()

	locale, err := time.LoadLocation(LOCALE_EST)

	if err != nil {
		log.Println(err)
	} else {
		return now.In(locale)
	}

	return now

} // getEstTimeNow


func getStatus() {

	today := getEstTimeNow()

	dgames, dplays := getDownloaded()

	//fmt.Println(today.Format(time.DateTime))
	fmt.Printf("\nSeason %s\n", stats.GetCurrentSeason())
	fmt.Println("-----------")
	fmt.Printf("Total Games:\t\t %d\n", getTotalGames())
	fmt.Printf("Downloaded Games:\t %d\n", dgames)
	fmt.Printf("Downloaded Plays:\t %d\n", dplays)
	fmt.Printf("Status Time:\t\t %s\n", today.Format(time.DateTime))

	getGameDayDetail()

	


	
} // getStatus
