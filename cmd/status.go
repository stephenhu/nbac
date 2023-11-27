package cmd

import (
	"fmt"
	"log"
	"os"
	//"path/filepath"
	//"sort"
	//"strings"
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


func getDownloaded() int {

	total := 0

	dirs, err := os.ReadDir(fDir)

	if err != nil {
		log.Println(err)
	} else {
	
		for _, day := range dirs {

			if day.IsDir() {

				games, err := os.ReadDir(fmt.Sprintf("%s/%s", fDir, day.Name()))

				if err != nil {
					log.Println(err)
				} else {
					// TODO: verify .json only
					total += len(games)
				}
	
			}

		}

	}

	return total

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
				return len(day.Games)
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


func getStatus() {

	today := time.Now()

	//fmt.Println(today.Format(time.DateTime))
	fmt.Printf("\nSeason %s\n", stats.GetCurrentSeason())
	fmt.Println("-----------")
	fmt.Printf("Total Games:\t\t %d\n", getTotalGames())
	fmt.Printf("Downloaded Games:\t %d\n", getDownloaded())
	fmt.Printf("Status Time:\t\t %s\n", today.Format(time.DateTime))

	getGameDayDetail()

	


	
} // getStatus
