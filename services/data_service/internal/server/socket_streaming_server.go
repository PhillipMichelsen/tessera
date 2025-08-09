package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
)

type SocketStreamingServer struct {
	manager *manager.Manager
}

func NewSocketStreamingServer(m *manager.Manager) *SocketStreamingServer {
	return &SocketStreamingServer{
		manager: m,
	}
}

// Serve accepts a listener (TCP or Unix) and begins handling incoming connections.
func (s *SocketStreamingServer) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *SocketStreamingServer) handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Failed to close connection: %v\n", err)
		} else {
			fmt.Println("Connection closed")
		}
	}(conn)
	reader := bufio.NewReader(conn)

	raw, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read stream UUID: %v\n", err)
		return
	}
	streamUUIDStr := strings.TrimSpace(raw)
	streamUUID, err := uuid.Parse(streamUUIDStr)
	if err != nil {
		fmt.Fprintf(conn, "Invalid stream UUID\n")
		return
	}

	outCh, err := s.manager.ConnectStream(streamUUID)
	if err != nil {
		fmt.Fprintf(conn, "Failed to connect to stream: %v\n", err)
		return
	}
	defer s.manager.DisconnectStream(streamUUID)

	for msg := range outCh {
		payload := struct {
			Provider string `json:"provider"`
			Subject  string `json:"subject"`
			Data     string `json:"data"`
		}{
			Provider: msg.Identifier.Provider,
			Subject:  msg.Identifier.Subject,
			Data:     fmt.Sprintf("%s", msg.Payload),
		}

		bytes, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("Failed to encode message: %v\n", err)
			continue
		}
		_, err = conn.Write(append(bytes, '\n'))
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Write error: %v\n", err)
			}
			break
		}
	}
}
