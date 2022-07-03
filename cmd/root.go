package cmd

import (

	"github.com/spf13/cobra"
)


const (
	DEFAULT_REDIS_HOST					= "127.0.0.1"
	DEFAULT_REDIS_PORT          = "6379"
	DEFAULT_REDIS_PROTOCOL      = "tcp"
)


const (
	TARGET_REDIS            = "redis"
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

	rootCmd.AddCommand(downCmd)
	rootCmd.AddCommand(loadCmd)


} // init


func Execute() error {
	return rootCmd.Execute()
} // Execute
