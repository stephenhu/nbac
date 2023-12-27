package cmd

import (

	"github.com/spf13/cobra"
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

	rootCmd.AddCommand(pullCmd)
	rootCmd.AddCommand(pushCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(versionCmd)

} // init


func Execute() error {
	return rootCmd.Execute()
} // Execute
