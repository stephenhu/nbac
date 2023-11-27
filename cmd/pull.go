package cmd

import (
	"encoding/json"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (

	pullCmd = &cobra.Command{
		Use: "pull",
		Short: "pull statistics",
		Long: "pull statistics from the NBA's APIs",
		Args: cobra.ExactArgs(1),
	}

)


func init() {

	pullCmd.PersistentFlags().StringVarP(&fYear, "year", "y", 
	  stats.GetCurrentSeason(), "Year to start download.  Default season is current")
	
	pullCmd.PersistentFlags().StringVarP(&fDir, "dir", "d", 
	  stats.GetCurrentSeason(), "Directory where data is stored")

	pullCmd.AddCommand(pullNbaCmd)
	pullCmd.AddCommand(pullBdlCmd)

	initDir()

} // init


func fileExists(d string) bool {

	_, err := os.Stat(d)

	if err != nil {
		
		if !os.IsNotExist(err) {
			log.Println(err)
		}

		return false

	} else {
		return true
	}

} // fileExists


func createDir(d string) {

	err := os.Mkdir(d, 0755)

	if err != nil && !os.IsNotExist(err) {
		log.Println(err)
	}

} // createDir


func initDir() {

	if !fileExists(fDir) {
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
			log.Println(err)
		}

	}

} // writeJson


func readJson(data interface{}, path string) {

	if len(path) == 0 {
		return
	}

	if fileExists(path) {

		buf, err := os.ReadFile(path)

		if err != nil {
			log.Println(err)
		} else {
			
			err := json.Unmarshal(buf, &data)

			if err != nil {
				log.Println(err)
			}

		}

	}

} // readJson
