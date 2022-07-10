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
	
	 pushCmd = &cobra.Command{
		Use: "push",
		Short: "push statistics",
		Long: "push statistics to data store",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

)


func init() {

	pushCmd.Flags().StringVarP(&fHost, "host", "", DEFAULT_REDIS_HOST,
    "Data store host address")
	pushCmd.Flags().StringVarP(&fPort, "port", "p", DEFAULT_REDIS_PORT,
    "Data store address port")

	pushCmd.AddCommand(redisCmd)

} // init
