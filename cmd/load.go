package cmd

import (
	//"encoding/json"
	//"fmt"
	//"os"
	//"sort"
	//"strings"

	//"github.com/PuerkitoBio/goquery"
	"github.com/spf13/cobra"
)


var (
	
	fHost           string
	fPort           string
	
	 loadCmd = &cobra.Command{
		Use: "load",
		Short: "load statistics",
		Long: "load statistics to data store",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

)


func init() {

	loadCmd.Flags().StringVarP(&fHost, "host", "", DEFAULT_REDIS_HOST,
    "Data store host address")
	loadCmd.Flags().StringVarP(&fPort, "port", "p", DEFAULT_REDIS_PORT,
    "Data store address port")

	loadCmd.AddCommand(redisCmd)

} // init
