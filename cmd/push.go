package cmd

import (
	//"encoding/json"
	//"fmt"
	//"log"
	//"os"
	//"path/filepath"
	//"sort"
	//"strings"

	//"github.com/PuerkitoBio/goquery"
	"github.com/spf13/cobra"
)


var (
	
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

	pushCmd.PersistentFlags().StringVarP(&fFrom, "from", "f", 
	  DEFAULT_PATH, "Path to read from")

	pushCmd.AddCommand(redisCmd)

} // init
