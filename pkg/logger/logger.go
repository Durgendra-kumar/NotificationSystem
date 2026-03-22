package logger

/*
Creates a structured JSON logger using Go's built-in slog package.

*/
/*
Read it, then retype it from scratch without copy-paste. Understand why slog and JSON format.
*/

// structured logging
import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const loggerKey contextKey = "logger"

// New creates a structured JSON logger — machine-readable in production.
func New() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// WithContext attaches a logger to a context.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// FromContext retrieves the logger from context.
// Falls back to a default logger if none is found.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return l
	}
	return New()
}
