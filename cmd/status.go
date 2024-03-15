package cmd

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (

	//fSched		*stats.NbaSchedule

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
} // init


func getTotalGames() int {

	total := 0

	for _, day := range schedule.LeagueSchedule.GameDates {
		total += len(day.Games)
	}

	return total

} // getTotalGames


func getDownloaded() (int, int) {
	return len(ScheduleIndex), len(PlaysIndex)
} // getDownloaded


func getLastDay(days []os.DirEntry) string {

	var lastDay *time.Time
	
	for _, d := range days {

		name := d.Name()

		t1, err := time.Parse(NBAC_DATE_FORMAT, name)

		if err != nil {
			log.Println(err)
		} else {

			if lastDay == nil {
				lastDay = &t1
			} else {

				if t1.After(*lastDay) {
					lastDay = &t1
				}

			}

		}

	}

	return lastDay.String()

} // getLastDay


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

	for _, day := range schedule.LeagueSchedule.GameDates {

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

	fmt.Println("\n\nDetailed Downloads by Day:")

	for _, day := range schedule.LeagueSchedule.GameDates {

		count := 0 

		for _, g := range day.Games {

			_, ok := ScheduleIndex[g.ID]

			if ok {
				count += 1
			}

		}

		t, err := time.Parse(stats.NBA_DATETIME_FORMAT, day.GameDate)

		if err != nil {
			log.Println(err)
		} else {

			fmt.Printf("%s:\t (%d/%d)\n", t.Format(stats.DATE_FORMAT), count,
				len(day.Games))

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

	loadSchedule(cy)

	LoadBlobIndexes()

	today := getEstTimeNow()

	dgames, dplays := getDownloaded()

	//fmt.Println(today.Format(time.DateTime))
	fmt.Printf("\nSeason %s\n", cy)
	fmt.Println("-----------")
	fmt.Printf("Total Games:\t\t %d\n", getTotalGames())
	fmt.Printf("Downloaded Games:\t %d\n", dgames)
	fmt.Printf("Downloaded Plays:\t %d\n", dplays)
	fmt.Printf("Status Time:\t\t %s\n", today.Format(time.DateTime))

	getGameDayDetail()
	
} // getStatus
