package server

import (
	"context"
	"time"

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
	return &GRPCControlServer{manager: m}
}

// StartStream creates a new session. It does NOT attach client channels.
// Your streaming RPC should later call AttachClient(sessionID, opts).
func (s *GRPCControlServer) StartStream(_ context.Context, req *pb.StartStreamRequest) (*pb.StartStreamResponse, error) {
	sessionID := s.manager.NewSession(time.Duration(1) * time.Minute) // timeout set to 1 minute
	return &pb.StartStreamResponse{StreamUuid: sessionID.String()}, nil
}

// ConfigureStream sets the session's subscriptions in one shot.
// It does NOT require channels to be attached.
func (s *GRPCControlServer) ConfigureStream(_ context.Context, req *pb.ConfigureStreamRequest) (*pb.ConfigureStreamResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	streamID, err := uuid.Parse(req.StreamUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream_uuid %q: %v", req.StreamUuid, err)
	}

	ids := make([]domain.Identifier, 0, len(req.Identifiers))
	for _, in := range req.Identifiers {
		id, e := domain.ParseIdentifier(in.Key)
		if e != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid identifier %q: %v", in.Key, e)
		}
		ids = append(ids, id)
	}

	if err := s.manager.ConfigureSession(streamID, ids); err != nil {
		// Map common manager errors to gRPC codes.
		switch err {
		case manager.ErrSessionNotFound:
			return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
		default:
			return nil, status.Errorf(codes.Internal, "set subscriptions: %v", err)
		}
	}
	return &pb.ConfigureStreamResponse{}, nil
}

// StopStream closes the session and tears down routes and streams.
func (s *GRPCControlServer) StopStream(_ context.Context, req *pb.StopStreamRequest) (*pb.StopStreamResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	streamID, err := uuid.Parse(req.StreamUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream_uuid %q: %v", req.StreamUuid, err)
	}

	if err := s.manager.CloseSession(streamID); err != nil {
		switch err {
		case manager.ErrSessionNotFound:
			return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
		default:
			return nil, status.Errorf(codes.Internal, "close session: %v", err)
		}
	}
	return &pb.StopStreamResponse{}, nil
}
