package cmd

import (
	"bytes"
	"encoding/json"
	"log"
	"os"

	"github.com/spf13/cobra"
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

	pullCmd.AddCommand(pullNbaCmd)

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


// deprecate
func write(buf []byte, b string, k string) {

	var out bytes.Buffer

	err := json.Indent(&out, buf, "", "  ")

	if err != nil {
		log.Println(err)
	} else {
		BlobPut(b, k, buf)
	}

} // write


func read(data interface{}, b string, k string) {

	if len(k) == 0 {
		return
	}

	blob := BlobGet(b, k)

	err := json.Unmarshal(blob, &data)

	if err != nil {
		log.Println(err)
	}

} // read
