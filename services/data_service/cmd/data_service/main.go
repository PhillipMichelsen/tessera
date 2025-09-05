package main

import (
	"fmt"
	"log"
	"net"

	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider/binance"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	fmt.Println("Starting Data Service...")
	// Setup
	r := router.NewRouter()
	m := manager.NewManager(r)
	binanceFutures := binance.NewFuturesWebsocket()
	m.AddProvider("binance_futures_websocket", binanceFutures)

	// gRPC Control Server
	grpcControlServer := grpc.NewServer()
	go func() {
		pb.RegisterDataServiceControlServer(grpcControlServer, server.NewGRPCControlServer(m))
		reflection.Register(grpcControlServer)
		grpcLis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("Failed to listen for gRPC control: %v", err)
		}
		log.Println("gRPC control server listening on :50051")
		if err := grpcControlServer.Serve(grpcLis); err != nil {
			log.Fatalf("Failed to serve gRPC control: %v", err)
		}
	}()

	// gRPC Streaming Server
	grpcStreamingServer := grpc.NewServer()
	go func() {
		pb.RegisterDataServiceStreamingServer(grpcStreamingServer, server.NewGRPCStreamingServer(m))
		reflection.Register(grpcStreamingServer)
		grpcLis, err := net.Listen("tcp", ":50052")
		if err != nil {
			log.Fatalf("Failed to listen for gRPC: %v", err)
		}
		log.Println("gRPC streaming server listening on :50052")
		if err := grpcStreamingServer.Serve(grpcLis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Socket Streaming Server
	/*
		socketStreamingServer := server.NewSocketStreamingServer(m)
		go func() {
			socketLis, err := net.Listen("tcp", ":6000")
			if err != nil {
				log.Fatalf("Failed to listen for socket: %v", err)
			}
			log.Println("Socket server listening on :6000")
			if err := socketStreamingServer.Serve(socketLis); err != nil {
				log.Fatalf("Socket server error: %v", err)
			}
		}()
	*/

	// Block main forever
	select {}
}
