package penalityFunction

import (
	"math"
)

type PenalityFunc func(int, int) int //must be overwritten

func StupidPenalityFunc(bufferPos int, currentValue int) int { //test only
	return currentValue - bufferPos
}

// exponential penalty: value * exp(-λ * bufferPos)
func ExpPenalty(lambda float64) PenalityFunc {
	return func(bufferPos int, currentValue int) int {
		val := float64(currentValue) * math.Exp(-lambda*float64(bufferPos))
		return int(math.Round(val))
	}
}

// polynomial penalty: value / (bufferPos+1)^α
func PolyPenalty(alpha float64) PenalityFunc {
	return func(bufferPos int, currentValue int) int {
		val := float64(currentValue) / math.Pow(float64(bufferPos+1), alpha)
		return int(math.Round(val))
	}
}

// logarithmic penalty: value / log(bufferPos + c)
func LogPenalty(c float64) PenalityFunc {
	return func(bufferPos int, currentValue int) int {
		// +2 to avoid division by zero
		log := math.Log(float64(bufferPos)+2) / math.Log(c)
		val := float64(currentValue) / log
		return int(math.Round(val))
	}
}
