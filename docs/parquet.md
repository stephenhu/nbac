# parquet

columnar data storage, good for long term storage, supports query of data.

* data will be stored either on blob storage of filesystem
* superset and other tools can be used to query the contents
* efficient storage, supports compression
* parquet files are immutable

## storage layout

statistics will be calculated on a daily basis, that means we will have a
point in time copy every day with each day accumulating the previous days'
statistics, these point in time copies can be deleted or kept, for example,
if you want to know the leading scorer during a certain week.

## schemas

* player stats per game
* team stats per game
* leaderboard (overall)
* leaderboard (rookies)
* standings
 