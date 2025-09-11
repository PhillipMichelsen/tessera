package main

import (
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/lmittmann/tint"
	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider/providers/test"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

	// Setup
	r := router.NewRouter(2048)
	m := manager.NewManager(r)
	testProvider := test.NewTestProvider(r.IncomingChannel(), time.Microsecond*50)
	if err := m.AddProvider("test_provider", testProvider); err != nil {
		slog.Error("add provider failed", "err", err)
		os.Exit(1)
	}

	// gRPC Control Server
	grpcControlServer := grpc.NewServer()
	go func() {
		pb.RegisterDataServiceControlServer(grpcControlServer, server.NewGRPCControlServer(m))
		reflection.Register(grpcControlServer)
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			slog.Error("listen failed", "cmp", "grpc-control", "addr", ":50051", "err", err)
			os.Exit(1)
		}
		slog.Info("listening", "cmp", "grpc-control", "addr", ":50051")
		if err := grpcControlServer.Serve(lis); err != nil {
			slog.Error("serve failed", "cmp", "grpc-control", "err", err)
			os.Exit(1)
		}
	}()

	// gRPC Streaming Server
	grpcStreamingServer := grpc.NewServer()
	go func() {
		pb.RegisterDataServiceStreamingServer(grpcStreamingServer, server.NewGRPCStreamingServer(m))
		reflection.Register(grpcStreamingServer)
		lis, err := net.Listen("tcp", ":50052")
		if err != nil {
			slog.Error("listen failed", "cmp", "grpc-streaming", "addr", ":50052", "err", err)
			os.Exit(1)
		}
		slog.Info("listening", "cmp", "grpc-streaming", "addr", ":50052")
		if err := grpcStreamingServer.Serve(lis); err != nil {
			slog.Error("serve failed", "cmp", "grpc-streaming", "err", err)
			os.Exit(1)
		}
	}()

	select {}
}
