package cmd


func percentage(attempted int, made int) float32 {

	if attempted == 0 {
		return 0.0
	} else {
		return float32(made)/float32(attempted)
	}

} // percentage


func percentageFp(attempted float32, made int) float32 {

	if attempted == 0 {
		return 0.0
	} else {
		return float32(made)/attempted
	}

} // percentageFp


func perGamePercentage(games int, d int) float32 {

	if games == 0 {
		return 0.0
	} else {
		return float32(d)/float32(games)
	}

} // perGamePercentage


func perGamePercentageFp(games int, d float32) float32 {

	if games == 0 {
		return 0.0
	} else {
		return d/float32(games)
	}

} // perGamePercentageFp

