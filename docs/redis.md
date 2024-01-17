# redis

## keys quick legend

key | fields
---|---
seasons | year
season:2023 | gameid
game:2023:gameid | home.teamid, away.teamid, home.p1, away.p1, home.total, away.total
game:2023:gameid:plays | action
player:2023:playerid | points, oreb, dreb
team:2023:teamid | points, oreb, dreb

## data structures

name | purpose | structure | key | value 
---|---|---|---|---
seasons | all seasons | hset | seasons | year
season.schedule | all games for a season | hset | year.schedule | game.id, date.est, away.team.id, home.team.id, away.team.code, home.team.code


## queries


### games

* all game's in a day, season:2023:date:20231026
* all game's in a season for a team season:2023:team:teamid
* all game's in playoff series for a team
* last N game's for a team
* live games
* unplayed games for a team

### comparisons

* team vs team
* player vs player
