package config_test

import (
	"testing"

	"github.com/sanjit-jeevanand/mini-kafka/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	cfg := config.Load()
	if cfg.AppName != "mini-kafka" {
		t.Errorf("expected app_name=mini-kafka, got %q", cfg.AppName)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected log_level=info, got %q", cfg.LogLevel)
	}
}
