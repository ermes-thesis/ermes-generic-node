package penalityFunction

type PenalityFunc func(int, int) int //must be overwritten

func StupidPenalityFunc(bufferPos int, currentValue int) int { //test only
	return currentValue - bufferPos
}
