package cmd

import (
  "github.com/stephenhu/stats"
)


var teams = stats.League{
	Conferences: []stats.Conference{
		stats.Conference{
			Name: "Eastern",
			Divisions: []stats.Division{
				stats.Division{
					Name: "Atlantic",
					Teams: []stats.TeamInfo{
						stats.TeamInfo{
							ID: 1610612738,
							Name: "Boston Celtics",
							Code: "BOS",
							City: "Boston",
							Abv: "Celtics",
						},
						stats.TeamInfo{
							ID: 1610612751,
							Name: "Brooklyn Nets",
						  Code: "BKN",
							City: "Brooklyn",
							Abv: "Nets",
						},
						stats.TeamInfo{
							ID: 1610612752,
							Name: "New York Knicks",
							Code: "NYK",
							City: "New York",
							Abv: "Knicks",
						},
						stats.TeamInfo{
							ID: 610612755,
							Name: "Philadelphia 76ers",
							Code: "PHI",
							City: "Philadelphia",
							Abv: "76ers",
						},
						stats.TeamInfo{
							ID: 1610612761,
							Name: "Toronto Raptors",
							Code: "TOR",
							City: "Toronto",
							Abv: "Raptors",
						},
					},
				},
				stats.Division{
					Name: "Central",
					Teams: []stats.TeamInfo{
						stats.TeamInfo{
							ID: 1610612741,
							Name: "Chicago Bulls",
							Code: "CHI",
							City: "Chicago",
							Abv: "Bulls",
						},
						stats.TeamInfo{
							ID: 1610612739,
							Name: "Cleveland Cavaliers",
							Code: "CLE",
							City: "Cleveland",
							Abv: "Cavaliers",
						},
						stats.TeamInfo{
						  ID: 1610612765,
							Name: "Detroit Pistons",
							Code: "DET",
							City: "Detroit",
							Abv: "Pistons",
						},
						stats.TeamInfo{	
							ID: 1610612754,
							Name: "Indiana Pacers",
							Code: "IND",
							City: "Indiana",
							Abv: "Pacers",
						},
						stats.TeamInfo{
							ID: 1610612749,
							Name: "Milwaukee Bucks",
							Code: "MIL",
							City: "Milwaukee",
							Abv: "Bucks",
						},
					},
				},
				stats.Division{
					Name: "Southeast",
					Teams: []stats.TeamInfo{
						stats.TeamInfo{
							ID: 1610612737,
							Name: "Atlanta Hawks",
							Code: "ATL",
							City: "Atlanta",
							Abv: "Hawks",
						},
						stats.TeamInfo{
							ID: 1610612766,
							Name: "Charlotte Hornets",
							Code: "CHA",
							City: "Charlotte",
							Abv: "Hornets",
						},
						stats.TeamInfo{
							ID: 1610612748,
							Name: "Miami Heat",
							Code: "MIA",
							City: "Miami",
							Abv: "Heat",
						},
						stats.TeamInfo{
							ID: 1610612753,
							Name: "Orlando Magic",
							Code: "ORL",
							City: "Orlando",
							Abv: "Magic",
						},
						stats.TeamInfo{
							ID: 1610612764,
							Name: "Washington Wizards",
							Code: "WAS",
							City: "Washington",
							Abv: "Wizards",
						},
					},
				},
			},
	  },
		stats.Conference{
			Name: "Western",
			Divisions: []stats.Division{
				stats.Division{
					Name: "Northwest",
					Teams: []stats.TeamInfo{
						stats.TeamInfo{
							ID: 1610612743,
							Name: "Denver Nuggets",
							Code: "DEN",
							City: "Denver",
							Abv: "Nuggets",
						},
						stats.TeamInfo{
							ID: 1610612750,
							Name: "Minnesota Timberwolves",
							Code: "MIN",
							City: "Minnesota",
							Abv: "Timberwolves",
						},
						stats.TeamInfo{
							ID: 1610612760,
							Name: "Oklahoma City Thunder",
							Code: "OKC",
							City: "Oklahoma City",
							Abv: "Thunder",
						},
						stats.TeamInfo{
							ID: 1610612757,
							Name: "Portland Trailblazers",
							Code: "POR",
							City: "Portland",
							Abv: "Trailblazers",
						},
						stats.TeamInfo{
							ID: 1610612762,
							Name: "Utah Jazz",
							Code: "UTA",
							City: "Utah",
							Abv: "Jazz",
						},
					},
				},
				stats.Division{
					Name: "Pacific",
					Teams: []stats.TeamInfo{
						stats.TeamInfo{
							ID: 1610612744,
							Name: "Golden State Warriors",
							Code: "GSW",
							City: "Golden State",
							Abv: "Warriors",
						},
						stats.TeamInfo{
							ID: 1610612746,
							Name: "LA Clippers",
							Code: "LAC",
							City: "LA",
							Abv: "Clippers",
						},
						stats.TeamInfo{
							ID: 1610612747,
							Name: "Los Angeles Lakers",
							Code: "LAL",
							City: "Los Angeles",
							Abv: "Lakers",
						},
						stats.TeamInfo{
							ID: 1610612756,
							Name: "Phoenix Suns",
							Code: "PHX",
							City: "Phoenix",
							Abv: "Suns",
						},
						stats.TeamInfo{
							ID: 1610612758,
							Name: "Sacremento Kings",
							Code: "SAC",
							City: "Sacremento",
							Abv: "Kings",
						},
					},
				},
				stats.Division{
					Name: "Southwest",
					Teams: []stats.TeamInfo{
						stats.TeamInfo{
							ID: 1610612742,
							Name: "Dallas Mavericks",
							Code: "DAL",
							City: "Dallas",
							Abv: "Mavericks",
						},
						stats.TeamInfo{
							ID: 1610612745,
							Name: "Houston Rockets",
							Code: "HOU",
							City: "Houston",
							Abv: "Rockets",
						},
						stats.TeamInfo{
							ID: 1610612763,
							Name: "Memphis Grizzlies",
							Code: "MEM",
							City: "Memphis",
							Abv: "Grizzlies",
						},
						stats.TeamInfo{
							ID: 1610612740,
							Name: "New Orleans Pelicans",
							Code: "NOP",
							City: "New Orleans",
							Abv: "Pelicans",
						},
						stats.TeamInfo{
							ID: 1610612759,
							Name: "San Antonio Spurs",
							Code: "SAS",
							City: "San Antonio",
							Abv: "Spurs",
						},
					},
				},
			},
		},
	},
}
