package logger

import (
	"fmt"
	"github.com/rs/zerolog"
)

type Factory struct {
	base Config
}

func NewFactory(base Config) *Factory {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05"
	return &Factory{base: base}
}

func (f *Factory) New(logFileName string) (*Logger, error) {
	cfg := f.base
	cfg.Output = &LogFile{
		LogDir:      f.base.Output.LogDir,
		LogFileName: fmt.Sprintf("%s.log", logFileName),
	}
	return NewLogger(cfg)
}

func (f *Factory) NewWith(logFileName string, opts func(*Config)) (*Logger, error) {
	cfg := f.base
	opts(&cfg)
	cfg.Output = &LogFile{
		LogDir:      f.base.Output.LogDir,
		LogFileName: fmt.Sprintf("%s.log", logFileName),
	}
	return NewLogger(cfg)
}
