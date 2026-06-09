// Package config provides 12-factor configuration via environment variables.
package config

import (
	"os"
	"strconv"
)

// Config holds all application configuration, read from environment variables.
// Prefix: MK_ (mini-kafka).
type Config struct {
	AppName  string
	LogLevel string
	LogJSON  bool

	// Phase 1+: storage
	DataDir string

	// Phase 2+: replication
	BrokerID  int
	BindAddr  string
	PeerAddrs string

	// Phase 3+: eval
	EvalResultsPath string
}

// Load reads configuration from environment variables with defaults.
func Load() Config {
	return Config{
		AppName:         getEnv("MK_APP_NAME", "mini-kafka"),
		LogLevel:        getEnv("MK_LOG_LEVEL", "info"),
		LogJSON:         getBoolEnv("MK_LOG_JSON", true),
		DataDir:         getEnv("MK_DATA_DIR", "data"),
		BrokerID:        getIntEnv("MK_BROKER_ID", 0),
		BindAddr:        getEnv("MK_BIND_ADDR", "0.0.0.0:9092"),
		PeerAddrs:       getEnv("MK_PEER_ADDRS", ""),
		EvalResultsPath: getEnv("MK_EVAL_RESULTS_PATH", "eval/results"),
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getBoolEnv(key string, defaultVal bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return defaultVal
	}
	return b
}

func getIntEnv(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return i
}
