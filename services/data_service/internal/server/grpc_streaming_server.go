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
	return &GRPCStreamingServer{manager: m}
}

// ConnectStream attaches a client to an existing session and streams outbound messages.
// This is server-streaming only; inbound use is optional and ignored here.
func (s *GRPCStreamingServer) ConnectStream(req *pb.ConnectStreamRequest, stream pb.DataServiceStreaming_ConnectStreamServer) error {
	if req == nil {
		return fmt.Errorf("nil request")
	}
	sessionID, err := uuid.Parse(req.StreamUuid)
	if err != nil {
		return fmt.Errorf("invalid UUID: %w", err)
	}

	// Defaults; tune or map from req if your proto carries options.
	opts := manager.ChannelOpts{
		InBufSize:    256,
		OutBufSize:   1024,
		DropOutbound: true, // do not let slow clients stall producers
		DropInbound:  true, // irrelevant here (we don't send inbound), safe default
	}

	_, out, err := s.manager.GetChannels(sessionID, opts)
	if err != nil {
		return fmt.Errorf("attach channels: %w", err)
	}
	defer func() { _ = s.manager.DetachClient(sessionID) }()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-out:
			if !ok {
				return nil // session closed
			}
			if err := stream.Send(&pb.Message{
				Identifier: &pb.Identifier{Key: msg.Identifier.Key()},
				Payload:    msg.Payload,
				Encoding:   string(msg.Encoding),
			}); err != nil {
				return err
			}
		}
	}
}
