# minio

integration with minio blob store allows nbac more freedom to run as a standalone job or service without needing downstream services that utilize the data created by nbac to not even really need to know about this service, although their needs.

## bucket structure

bucket | description
--- | ---
2023.nba.raw | contains all raw downloaded json from nba.com like boxscores, schedules per season
2023.nba.analytics | contains all transformed data from ETL jobs such as parquet files

## blob names

will use a flat structure to store files in nba, therefore, a unique name 
needs to be generated:


blob | description
--- | ---
`0022300061.json` | boxscore (season)
`schedule.json` | season schedule
`20230503.players.parquet` | all player stats (season)
`20230503.games.parquet` | all game stats
`20230503.leaders.parquet` | aggregated player stats
`20230503.plays.parquet` | all plays 


## bucket creation

buckets need to be seeded, it is not the expectation that
nbac create buckets when it doesn't exist TODO: this needs some
investigation

## sync process

* check for existence of blob
* download and store if not available
* do we want to create an easy index in sqlite?  let's not do that since it would make nbac stateful
