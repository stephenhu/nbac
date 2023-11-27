package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)


var (
	
	versionCmd = &cobra.Command{
		Use: "version",
		Short: "version",
		Long: "version",
		Run: func(cmd *cobra.Command, args []string) {
			version()
		},
	}

)


func init() {
} // init


func version() {
	fmt.Printf("v%s\n", APP_VERSION)
} // version
