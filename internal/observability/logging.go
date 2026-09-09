package observability

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Logger = zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
}

func New(env string) Logger {
	var w io.Writer = os.Stdout
	var lvl zerolog.Level = zerolog.InfoLevel

	if env == "development" {
		w = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
		lvl = zerolog.DebugLevel
	}

	lg := zerolog.New(w).
		Level(lvl).
		With().
		Timestamp().
		Logger()

	log.Logger = lg
	return lg
}
