package server

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCControlServer struct {
	pb.UnimplementedDataServiceControlServer
	manager *manager.Manager
}

func NewGRPCControlServer(m *manager.Manager) *GRPCControlServer {
	return &GRPCControlServer{
		manager: m,
	}
}

func (s *GRPCControlServer) StartStream(_ context.Context, _ *pb.StartStreamRequest) (*pb.StartStreamResponse, error) {
	streamID, err := s.manager.StartStream()
	if err != nil {
		return nil, fmt.Errorf("failed to start stream: %w", err)
	}

	return &pb.StartStreamResponse{StreamUuid: streamID.String()}, nil
}

func (s *GRPCControlServer) ConfigureStream(_ context.Context, req *pb.ConfigureStreamRequest) (*pb.ConfigureStreamResponse, error) {
	streamID, err := uuid.Parse(req.StreamUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream_uuid %q: %v", req.StreamUuid, err)
	}

	// Transform identifiers from protobuf to domain format
	var ids []domain.Identifier
	for _, i := range req.Identifiers {
		ids = append(ids, domain.Identifier{
			Provider: i.Provider,
			Subject:  i.Subject,
		})
	}

	if err := s.manager.ConfigureStream(streamID, ids); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "configure failed: %v", err)
	}

	return &pb.ConfigureStreamResponse{}, nil
}

func (s *GRPCControlServer) StopStream(_ context.Context, req *pb.StopStreamRequest) (*pb.StopStreamResponse, error) {
	streamID, err := uuid.Parse(req.StreamUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream_uuid %q: %v", req.StreamUuid, err)
	}

	err = s.manager.StopStream(streamID) // Should only error if the stream doesn't exist
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to stop stream: %v", err)
	}

	return &pb.StopStreamResponse{}, nil
}
