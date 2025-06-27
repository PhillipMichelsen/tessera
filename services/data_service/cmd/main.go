package main

import (
	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider/binance"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

func main() {
	// Setup
	r := router.NewRouter()
	m := manager.NewManager(r)
	binanceFutures := binance.NewFuturesWebsocket()
	m.AddProvider("binance_futures_websocket", binanceFutures)

	// gRPC Server
	grpcServer := grpc.NewServer()
	streamingServer := server.NewGRPCStreamingServer(m)
	pb.RegisterDataServiceStreamingServer(grpcServer, streamingServer)
	reflection.Register(grpcServer)

	go func() {
		grpcLis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("Failed to listen for gRPC: %v", err)
		}
		log.Println("gRPC server listening on :50051")
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Socket Server
	socketServer := server.NewSocketStreamingServer(m)
	go func() {
		socketLis, err := net.Listen("tcp", ":6000")
		if err != nil {
			log.Fatalf("Failed to listen for socket: %v", err)
		}
		log.Println("Socket server listening on :6000")
		if err := socketServer.Serve(socketLis); err != nil {
			log.Fatalf("Socket server error: %v", err)
		}
	}()

	// Block main forever
	select {}
}
