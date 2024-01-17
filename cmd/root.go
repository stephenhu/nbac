package cmd

import (

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


const (
	APP_VERSION						= "0.1"
  DEFAULT_PATH					= "."
	PBP_SUFFIX           	= "playbyplay"
	LOCALE_EST            = "EST"
)

const (
	FROM_SEASON_BEGIN			= ""
	STR_EMPTY             = ""
	STR_INDENT            = "  "
)


const (
	EXT_JSON							= ".json"
)


var (

	fLocation			string
	fDir					string					= DEFAULT_PATH
	fFrom					string
	fYear       	string

	rootCmd = &cobra.Command{
		Use: "nbac",
		Short: "nbac command line tool",
		Long: "nbac is a command line tool for downloading NBA statistics",
		Version: "0.1",
	}

)


func init() {

	cobra.OnInitialize()

	rootCmd.PersistentFlags().StringVarP(&fDir, "dir", "d", 
		stats.GetCurrentSeason(), "Directory where data is stored")

	rootCmd.AddCommand(pullCmd)
	rootCmd.AddCommand(pushCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(generateCmd)

} // init


func Execute() error {
	return rootCmd.Execute()
} // Execute
