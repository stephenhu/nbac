# design

* download all (i.e. since 1979)
* download an entire season
* download a game
* download storage format (e.g. csv, json)

## pull

* `nbac pull bdl` # balldontlie.io (1979 - now)
* `nbac pull nba` # data.nba.net (only supports 2014 - now)
* `nbac pull --season [YYYY]` # entire season stats
* `nbac pull game` # single game stats

## push

* `nbac push redis` # redis, postgresql, sqlite3, csv
