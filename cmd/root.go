package cmd

import (

	"github.com/spf13/cobra"
)


const (
  DEFAULT_PATH					= "."
)


const (
	EXT_JSON							= ".json"
)


var (

	fLocation			string

	rootCmd = &cobra.Command{
		Use: "nbac",
		Short: "nbac command line tool",
		Long: "nbac is a command line tool for downloading and transforming NBA statistics",
		Version: "0.1",
	}

)


func init() {

	cobra.OnInitialize()

	rootCmd.AddCommand(pullCmd)
	rootCmd.AddCommand(pushCmd)

} // init


func Execute() error {
	return rootCmd.Execute()
} // Execute
