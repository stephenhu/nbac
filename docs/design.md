# design

there should be a job that pulls data from data sources, this data can be saved in multiple formats such as json, redis, converted to golang structs, in memory, etc.  the job should be able to pull data in different methods such as for a certain day or an entire season.

data pulls can overwrite or append, should be this optionality

there are several data sources which seem to change over time, but the most reliable is from the official nba.

the entry point to nba data is based on season, seems data before 2015 has been purposely made unavailable and now only the current season data is provided.

## components

* downloader: pulls from various sources, preserves data to standard format (json)
* analyzer: provides stats analysis, articles generation
* main: service to manage downloads, generate articles, analysis, perhaps automated podcast?
* 

* download all (i.e. since 1979)
* download an entire season
* download a game
* download storage format (e.g. csv, json)

## pull

* `nbac pull bdl` # balldontlie.io (should deprecate, this api is not compatible with nba data i.e. gameId's and playerId's do not match)
* `nbac pull nba` # cdn.nba.com (only supports current season)
* `nbac pull --season [YYYY]` # entire season stats (should deprecate this)
* `nbac pull game` # single game stats


## push

* `nbac push redis` # redis, postgresql, sqlite3, csv
