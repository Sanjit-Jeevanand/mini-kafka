package logger_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/sanjit-jeevanand/mini-kafka/internal/logger"
)

func TestRequestIDRoundtrip(t *testing.T) {
	ctx := logger.WithRequestID(context.Background(), "abc-123")
	if got := logger.RequestIDFromContext(ctx); got != "abc-123" {
		t.Errorf("expected abc-123, got %q", got)
	}
}

func TestNewLogger(t *testing.T) {
	l := logger.New(slog.LevelInfo, true)
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
}
