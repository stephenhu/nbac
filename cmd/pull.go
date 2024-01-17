package cmd

import (
	"bytes"
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

	pullCmd.AddCommand(pullNbaCmd)

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


func write(buf []byte, path string) {

	var out bytes.Buffer

	err := json.Indent(&out, buf, "", "  ")

	if err != nil {
		log.Println(err)
	} else {

		err := os.WriteFile(path, out.Bytes(), 0660)

		if err != nil {
			log.Println(err)
		}				
	
	}

} // write


func read(data interface{}, path string) {

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

} // read
