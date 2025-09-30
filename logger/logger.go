package logger

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Logger struct {
	Logger  zerolog.Logger
	logFile *os.File
}

type Config struct {
	Level         zerolog.Level
	Format        string // "text" or "json"
	Output        *LogFile
	StdoutEnabled bool
	AddSource     bool
	Colored       bool
}

type LogFile struct {
	LogDir      string
	LogFileName string
}

const levelSectionWidth = 7

func NewLogger(config Config) (*Logger, error) {
	var (
		outputs []io.Writer
		logFile *os.File
		err     error
	)

	// Checking if an output .log file has been configured
	if config.Output != nil {
		logPath := filepath.Join(config.Output.LogDir, config.Output.LogFileName)
		err = os.MkdirAll(filepath.Dir(logPath), 0755)
		if err != nil {
			return nil, err
		}
		logFile, err = os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}

		// If everything went well, add the file to the outputs to which the logger must write
		outputs = append(outputs, buildWriter(config.Format, logFile, true))
	}

	// If an output file has not been configured, fallback to stdout
	// If an output file has been configured, add stdout only if explicitly requested
	if len(outputs) == 0 || config.StdoutEnabled {
		outputs = append(outputs, buildConsoleWriter(os.Stdout, !config.Colored))
	}

	// Notify zerolog to use multiple writers
	writer := zerolog.MultiLevelWriter(outputs...)

	// Initializing the base structure of the logger with a timestamp
	base := zerolog.New(writer).With().Timestamp()

	// If requested add the Caller file:line
	if config.AddSource {
		base = base.CallerWithSkipFrameCount(3)
	}

	// Create the logger
	logger := base.Logger().Level(config.Level)
	return &Logger{
		Logger:  logger,
		logFile: logFile,
	}, nil

}

// buildConsoleWriter wraps the parameter writer into a zerolog.ConsoleWriter to
// create some pretty logging. This is the default writer that is used to write to
// os.Stdout
func buildConsoleWriter(out io.Writer, noColor bool) io.Writer {
	return diode.NewWriter(zerolog.ConsoleWriter{
		Out:        out,
		NoColor:    noColor,
		TimeFormat: "2006-01-02 15:04:05",
		PartsOrder: []string{zerolog.LevelFieldName, zerolog.TimestampFieldName, zerolog.CallerFieldName, zerolog.MessageFieldName},
		FormatLevel: func(level any) string {
			levelSection := fmt.Sprintf("[%s]", level)
			pad := levelSectionWidth - len(levelSection)
			levelSection = levelSection + strings.Repeat(" ", pad)
			return strings.ToUpper(levelSection)
		},
	}, 1000, 10*time.Millisecond, func(missed int) {
		fmt.Printf("Logger dropped %d messages", missed)
	})
}

// buildWriter is used to build a writer according to the specified format.
//
// If the "json" format is chosen, the parameter writer is returned as it is, and zerolog
// will take care of its formatting (JSON, is the default formatting for zerolog logging)
//
// If the "text" format is chosen, the parameter writer is wrapper in a zerolog.ConsoleWriter
// that will take care of formatting the logging messages in a pretty and easily understandable way
func buildWriter(format string, out io.Writer, noColor bool) io.Writer {
	if format == "json" {
		return diode.NewWriter(out, 1000, 10*time.Millisecond, func(missed int) {
			fmt.Printf("Logger dropped %d messages", missed)
		})
	}

	return buildConsoleWriter(out, noColor)
}

func (l *Logger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

func (l *Logger) Info(msg string, attrs ...any) {
	l.Logger.Info().Fields(attrs).Msg(msg)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.Logger.Info().Msg(fmt.Sprintf(format, args...))
}

func (l *Logger) InfofWith(format string, args []any, attrs ...any) {
	l.Logger.Info().Fields(attrs).Msgf(format, args...)
}

func (l *Logger) Error(msg string, attrs ...any) {
	l.Logger.Error().Fields(attrs).Msg(msg)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Logger.Error().Msgf(format, args...)
}

func (l *Logger) ErrorfErr(format string, args ...interface{}) error {
	errStr := fmt.Sprintf(format, args...)
	l.Logger.Error().Msg(errStr)

	return errors.New(errStr)
}

func (l *Logger) Debug(msg string, attrs ...any) {
	l.Logger.Debug().Fields(attrs).Msg(msg)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.Logger.Debug().Msgf(format, args...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Logger.Warn().Msgf(format, args...)
}

func (l *Logger) DebugfWith(format string, args []any, attrs ...any) {
	l.Logger.Debug().Fields(attrs).Msgf(format, args...)
}

func (l *Logger) Warn(msg string, attrs ...any) {
	l.Logger.Warn().Fields(attrs).Msg(msg)
}

func (l *Logger) WarnfWith(format string, args []any, attrs ...any) {
	l.Logger.Warn().Fields(attrs).Msgf(format, args...)
}

func (l *Logger) ErrorfWith(format string, args []any, attrs ...any) {
	l.Logger.Error().Fields(attrs).Msgf(format, args...)
}

func (l *Logger) ErrorfErrWith(format string, args []any, attrs ...any) error {
	errStr := fmt.Sprintf(format, args...)

	l.Logger.Error().Fields(attrs).Msg(errStr)
	return errors.New(errStr)
}

func (l *Logger) Enabled(level zerolog.Level) bool {
	return level >= l.Logger.GetLevel()
}

func (l *Logger) With(args ...any) *Logger {
	return &Logger{
		Logger:  l.Logger.With().Fields(args).Logger(),
		logFile: l.logFile,
	}
}
