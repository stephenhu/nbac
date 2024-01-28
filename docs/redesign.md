# redesign

currently uses stats library which downloads the json file, parses it to golang data structure and then stores to json.  why not just store directly to json on the fs in the original format?  in this way, stats library can be used to manage data in memory and used for higher level calculations and forth.

actually, not saving the raw json has its advantages since we're filtering a lot of the unwanted information, this in some senses is an etl job, but maybe storing the original will have advantages since the data will be more and the optionality of taking this data will help provide more value later on perhaps.

* download latest boxscore and schedule information
* calculate latest leaderboards and standings, this can either be effectively calculated
everyday and re-calculated from the beginning or it can be accumulated.

some data is pretty static like, for example, teams, divisions, conferences.

also, we should be cautious of what sort of data gets into warehouse versus smaller data
sets like team information.  general criteria for warehouse data:

1. if the dataset is large, 100+ rows
1. if the dataset requires sophisticated queries or number crunching and transformation

if not then use memory.

note that some data doesn't even change across seasons, like team conferences/divisions.


## generate stats

### directory structure

2023
data (warehouse)
  games.20240122.parquet (contains all boxscores)
  players.20240122.parquet
  teams.20240122.parquet
  standings.20240122.parquet
  leaders.20240122.parquet
