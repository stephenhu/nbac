package cmd

import (

	"github.com/spf13/cobra"
)


var (

	fLocation			string

	rootCmd = &cobra.Command{
		Use: "nbac",
		Short: "nbac command line tool",
		Long: "nbac is a command line tool for crawling NBA statistics",
		Version: "0.1",
	}

)


func init() {

	cobra.OnInitialize()

	rootCmd.AddCommand(downloadCmd)


} // init


func Execute() error {
	return rootCmd.Execute()
} // Execute
