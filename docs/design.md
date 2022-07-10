# design

* download all (i.e. since 1979)
* download an entire season
* download a game
* download storage format (e.g. csv, json)

## down

* `nbac down` # all games, players, teams
* `nbac down list`
* `nbac down bdl` # balldontlie.io (1979 - now)
* `nbac down nba` # data.nba.net (only supports 2014 - now)
* `nbac down --season [YYYY]` # entire season stats
* `nbac down game` # single game stats

## 

* `nbac transform --target redis` # redis, postgresql, sqlite3, csv
