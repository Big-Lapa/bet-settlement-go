// Package config loads settings from environment variables (12-factor).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings.
type Config struct {
	HTTPAddr        string
	Storage         string // "postgres" or "memory"
	DatabaseURL     string
	KafkaBrokers    []string
	ResultsTopic    string
	SettledTopic    string
	ConsumerGroup   string
	Workers         int
	BatchSize       int
	BatchWait       time.Duration
	OutboxInterval  time.Duration
	OutboxBatch     int
	ShutdownTimeout time.Duration
	LogLevel        string
}

// Load reads the environment and validates the result.
func Load() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		Storage:         env("STORAGE", "postgres"),
		DatabaseURL:     env("DATABASE_URL", ""),
		KafkaBrokers:    strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		ResultsTopic:    env("RESULTS_TOPIC", "match-results"),
		SettledTopic:    env("SETTLED_TOPIC", "bet-settled"),
		ConsumerGroup:   env("CONSUMER_GROUP", "bet-settlement"),
		Workers:         envInt("WORKERS", 8, &errs),
		BatchSize:       envInt("BATCH_SIZE", 100, &errs),
		BatchWait:       envDuration("BATCH_WAIT", 500*time.Millisecond, &errs),
		OutboxInterval:  envDuration("OUTBOX_INTERVAL", time.Second, &errs),
		OutboxBatch:     envInt("OUTBOX_BATCH", 100, &errs),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 10*time.Second, &errs),
		LogLevel:        env("LOG_LEVEL", "info"),
	}

	switch c.Storage {
	case "postgres":
		if c.DatabaseURL == "" {
			errs = append(errs, errors.New("DATABASE_URL is required when STORAGE=postgres"))
		}
	case "memory":
	default:
		errs = append(errs, fmt.Errorf("STORAGE must be postgres or memory, got %q", c.Storage))
	}
	if c.Workers < 1 || c.BatchSize < 1 || c.OutboxBatch < 1 {
		errs = append(errs, errors.New("WORKERS, BATCH_SIZE and OUTBOX_BATCH must be positive"))
	}
	// errors.Join reports every problem at once instead of one per restart.
	return c, errors.Join(errs...)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int, errs *[]error) int {
	v := env(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func envDuration(key string, def time.Duration, errs *[]error) time.Duration {
	v := env(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}
