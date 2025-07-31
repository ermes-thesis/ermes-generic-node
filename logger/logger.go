package logger

import (
	"fmt"
	"io"
	"sync"
)

type LoggerLevel int

const (
	Pending LoggerLevel = iota
	ERROR
	WARNING
	INFO
)

type Logger struct {
	currentLoggerLevel LoggerLevel
	loggerLock         *sync.Mutex
	logger             *io.Writer
}

func NewLogger(loggerLevel LoggerLevel, logger *io.Writer) *Logger {
	return &Logger{
		currentLoggerLevel: loggerLevel,
		loggerLock:         &sync.Mutex{},
		logger:             logger,
	}
}

func (l *Logger) Println(structName string, message string, messageLevel LoggerLevel) {
	if messageLevel <= l.currentLoggerLevel {
		l.loggerLock.Lock()
		defer l.loggerLock.Unlock()

		fmt.Fprintf(*l.logger, "[%s] (%s) - %s\n", structName, l.levelToString(messageLevel), message)
	}
}

// Metodo per convertire i livelli in stringa
func (l *Logger) levelToString(level LoggerLevel) string {
	switch level {
	case ERROR:
		return "ERROR"
	case WARNING:
		return "WARNING"
	case INFO:
		return "INFO"
	default:
		return "UNKNOWN"
	}
}
