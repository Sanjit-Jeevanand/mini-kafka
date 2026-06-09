// Package logger configures structured JSON logging with request_id propagation.
package logger

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// WithRequestID returns a context carrying the given request ID.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// RequestIDFromContext extracts the request ID from the context, or returns empty string.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// New returns a JSON slog.Logger writing to stdout.
// Pass json=false for human-readable output in development.
func New(level slog.Level, json bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	if json {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}

// With returns a logger with request_id attached, read from ctx.
func With(logger *slog.Logger, ctx context.Context) *slog.Logger {
	if id := RequestIDFromContext(ctx); id != "" {
		return logger.With("request_id", id)
	}
	return logger
}
