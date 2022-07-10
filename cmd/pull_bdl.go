package cmd

import (

	"github.com/spf13/cobra"
)


var (
	
	pullBdlCmd = &cobra.Command{
		Use: "bdl",
		Short: "bdl statistics",
		Long: "bdl statistics from the open source data set at balldontlie.io",
		Run: func(cmd *cobra.Command, args []string) {
		},
	}

)
