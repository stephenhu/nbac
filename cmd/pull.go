package cmd

import (	
	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


var (
	
	fSeason       int

	pullCmd = &cobra.Command{
		Use: "pull",
		Short: "pull statistics",
		Long: "pull statistics from the NBA's APIs",
		Args: cobra.ExactArgs(1),
	}

)


func init() {

	pullCmd.PersistentFlags().IntVarP(&fSeason, "season", "s", 
	  stats.YEAR_MODERN_ERA, "Year to start download.  Default season is 1979")

	pullCmd.AddCommand(pullNbaCmd)
	pullCmd.AddCommand(pullBdlCmd)

} // init
