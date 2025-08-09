package server

import (
	"fmt"

	"github.com/google/uuid"
	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
)

type GRPCStreamingServer struct {
	pb.UnimplementedDataServiceStreamingServer
	manager *manager.Manager
}

func NewGRPCStreamingServer(m *manager.Manager) *GRPCStreamingServer {
	return &GRPCStreamingServer{
		manager: m,
	}
}

func (s *GRPCStreamingServer) ConnectStream(req *pb.ConnectStreamRequest, stream pb.DataServiceStreaming_ConnectStreamServer) error {
	streamUUID, err := uuid.Parse(req.StreamUuid)
	if err != nil {
		return fmt.Errorf("invalid UUID: %w", err)
	}

	ch, err := s.manager.ConnectStream(streamUUID)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	for {
		select {
		case <-stream.Context().Done():
			s.manager.DisconnectStream(streamUUID)
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}

			err := stream.Send(&pb.Message{
				Identifier: &pb.Identifier{
					Provider: msg.Identifier.Provider,
					Subject:  msg.Identifier.Subject,
				},
				Payload:  msg.Payload,
				Encoding: string(msg.Encoding),
			})

			if err != nil {
				return err
			}
		}
	}
}
