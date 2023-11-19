package cmd

import (
	"encoding/json"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	fYear       	int
	fDir          string				= DEFAULT_PATH

	pullCmd = &cobra.Command{
		Use: "pull",
		Short: "pull statistics",
		Long: "pull statistics from the NBA's APIs",
		Args: cobra.ExactArgs(1),
	}

)


func init() {

	pullCmd.PersistentFlags().IntVarP(&fYear, "year", "y", 
	  stats.YEAR_MODERN_ERA, "Year to start download.  Default season is current")
	
	pullCmd.PersistentFlags().StringVarP(&fDir, "dir", "d", 
	  stats.GetCurrentSeason(), "Directory where data is stored")

	pullCmd.AddCommand(pullNbaCmd)
	pullCmd.AddCommand(pullBdlCmd)

	initDir()

} // init


func dirExists(d string) bool {

	_, err := os.Stat(d)

	if err != nil {
		
		if !os.IsNotExist(err) {
			log.Println(err)
		}

		return false

	} else {
		return true
	}

} // dirExists


func createDir(d string) {

	err := os.Mkdir(d, 0755)

	if err != nil && !os.IsNotExist(err) {
		log.Println(err)
	}

} // createDir


func initDir() {

	if !dirExists(fDir) {
		createDir(fDir)
	}

} // initDir


func writeJson(data interface{}, path string) {

	j, err := json.MarshalIndent(data, stats.STRING_EMPTY,
		stats.STRING_TAB)

	if err != nil {
		log.Println(err)
	} else {

		err := os.WriteFile(path, j, 0660)

		if err != nil {
			log.Println("big nut")
			log.Println(err)
		}

	}

} // writeJson
