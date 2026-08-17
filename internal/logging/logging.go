// Package logging builds the process-wide structured logger.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewJSON creates a JSON logger whose minimum level is controlled by
// LOG_LEVEL. Supported values are debug, info, warn and error; unknown or
// empty values keep the production-safe info default.
func NewJSON(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: parseLevel(os.Getenv("LOG_LEVEL")),
	}))
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
