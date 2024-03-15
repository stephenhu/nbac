package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/stephenhu/stats"
)


const (
	CORE_DIR             	= "core"
	WAREHOUSE_DIR					= "data"
	PLAYBYPLAY            = "playbyplay.json"
	SCHEDULE              = "schedule.json"
)


var (

	generateCmd = &cobra.Command{
		Use: "generate",
		Short: "calculate statistics",
		Long: "calculate statistics like leaderboards, standings",
		Args: cobra.ExactArgs(1),
		
	}

)


func init() {

	generateCmd.AddCommand(generateDataCmd)

} // init


func getDir(p string) []os.DirEntry {

	dirs, err := os.ReadDir(p)

	if err != nil {
		
		log.Println(err)

		return nil

	} else {
		return dirs
	}

} // getDir


func initWarehouseDir() {

	if fileExists(fDir) {
		
		wh := filepath.Join(fDir, WAREHOUSE_DIR)

		createDir(wh)

	}

} // initWarehouseDir


func getBoxscore(f string) *stats.NbaBoxscore {

	s := stats.NbaBoxscore{}

	buf, err := os.ReadFile(f)

	if err != nil {
		
		log.Println(err)
		return nil
	
		} else {

		err := json.Unmarshal(buf, &s)

		if err != nil {
			
			log.Println(err)
			return nil

		} else {
			return &s
		}

	}

} // getBoxscore


func parseBoxscores() []stats.NbaBoxscore {

	scores := []stats.NbaBoxscore{}

	raw := BucketRaw(cy)

	for k, _ := range ScheduleIndex {
		
		buf := BlobGet(raw, fmt.Sprintf("%s%s", k, EXT_JSON))

		box := stats.NbaBoxscore{}

		if len(box.Game.ID) == 0 {
			continue
		}

		err := json.Unmarshal(buf, &box)

		if err != nil {
			log.Println(err)
		} else {
			scores = append(scores, box)
		}

	}

	return scores

} // parseBoxscores


func parseBoxscores2() []stats.NbaBoxscore {

	scores := []stats.NbaBoxscore{}

	dirs := getDir(fDir)

	for _, dir := range dirs {

		fn := filepath.Join(fDir, dir.Name())

		gameDays := getDir(fn)

		for _, box := range gameDays {

			if !strings.Contains(box.Name(), PLAYBYPLAY) &&
			  !strings.Contains(box.Name(), SCHEDULE) &&
				filepath.Ext(box.Name()) == EXT_JSON {

				file := filepath.Join(fn, box.Name())

				s := getBoxscore(file)
			
				scores = append(scores, *s)

			}

		}

	}

	return scores

} // parseBoxscores
