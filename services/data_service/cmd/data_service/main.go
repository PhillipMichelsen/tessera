package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
)

func initLogger() *slog.Logger {
	level := parseLevel(env("LOG_LEVEL", "debug"))
	if env("LOG_FORMAT", "pretty") == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		}))
	}
	return slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      level,
		TimeFormat: time.RFC3339Nano,
		NoColor:    os.Getenv("NO_COLOR") != "",
	}))
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	slog.SetDefault(initLogger())
	slog.Info("starting", "svc", "data-service")

	select {}
}
