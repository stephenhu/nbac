# backlog

* print meaningful output for events like file written
* file overwrite
* defect: uses local time instead of est or pst
* gameTime is returning empty for parquet games
* nbac status when no schedule.json
* schedule.json seems to get checked with any command
* current resume download is broken, it is based on dir being available, but the dir could be empty or partially full?
* resume should be based on status, status should provide a diff of missing files and that's what should be downloaded
* ~~for leaderboard, don't count preseason and playoff games~~
* count leaderboard for regular season and playoffs, including in-season
* player, assists to turnover ratio
* ~~usage: nbac generate data~~
* ~~resume sync~~
* ~~file check~~
* use library helpers for Pct
* major issue with incomplete scores, so if a game is in progress, the json is available, but because the file is downloaded, it is considered finished (gameStatusText shows "Final")