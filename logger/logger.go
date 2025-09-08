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
	allowList          map[string]bool
}

func NewLogger(loggerLevel LoggerLevel, logger *io.Writer) *Logger {
	return &Logger{
		currentLoggerLevel: loggerLevel,
		loggerLock:         &sync.Mutex{},
		logger:             logger,
		allowList:          make(map[string]bool),
	}
}

func (l *Logger) SetAllowList(structNames []string) {
	l.loggerLock.Lock()
	defer l.loggerLock.Unlock()

	// Reset della mappa
	l.allowList = make(map[string]bool)

	// Popola la mappa con i nomi consentiti
	for _, name := range structNames {
		l.allowList[name] = true
	}
}

// ClearAllowList rimuove l'allow list, permettendo a tutti i messaggi di passare
func (l *Logger) ClearAllowList() {
	l.loggerLock.Lock()
	defer l.loggerLock.Unlock()

	l.allowList = make(map[string]bool)
}

// isStructAllowed verifica se il structName è consentito dall'allow list
func (l *Logger) isStructAllowed(structName string) bool {
	// Se l'allow list è vuota, tutti i messaggi sono consentiti
	if len(l.allowList) == 0 {
		return true
	}

	// Controlla se il structName è nella allow list
	return l.allowList[structName]
}

func (l *Logger) Println(structName string, message string, messageLevel LoggerLevel) {
	if messageLevel <= l.currentLoggerLevel {
		l.loggerLock.Lock()
		defer l.loggerLock.Unlock()

		// Controlla se il structName è consentito
		if !l.isStructAllowed(structName) {
			return
		}

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
