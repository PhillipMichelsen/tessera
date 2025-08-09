package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/google/uuid"
	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
	"google.golang.org/protobuf/proto"
)

type SocketStreamingServer struct {
	manager *manager.Manager
}

func NewSocketStreamingServer(m *manager.Manager) *SocketStreamingServer {
	return &SocketStreamingServer{manager: m}
}

// Accepts connections and hands each off to handleConnection.
func (s *SocketStreamingServer) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("accept error: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *SocketStreamingServer) handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("conn close error: %v\n", err)
		} else {
			fmt.Println("connection closed")
		}
	}()

	// Low-latency socket hints (best-effort).
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetWriteBuffer(512 * 1024)
		_ = tc.SetReadBuffer(512 * 1024)
	}

	reader := bufio.NewReader(conn)

	// Protocol header: first line is the stream UUID.
	raw, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("read stream UUID error: %v\n", err)
		_, _ = fmt.Fprint(conn, "Failed to read stream UUID\n")
		return
	}
	streamUUIDStr := strings.TrimSpace(raw)
	streamUUID, err := uuid.Parse(streamUUIDStr)
	if err != nil {
		_, _ = fmt.Fprint(conn, "Invalid stream UUID\n")
		return
	}

	outCh, err := s.manager.ConnectStream(streamUUID)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "Failed to connect to stream: %v\n", err)
		return
	}
	defer s.manager.DisconnectStream(streamUUID)

	writer := bufio.NewWriterSize(conn, 256*1024)
	defer func(writer *bufio.Writer) {
		err := writer.Flush()
		if err != nil {
			fmt.Printf("final flush error: %v\n", err)
		}
	}(writer)

	const flushEvery = 32
	batch := 0

	for msg := range outCh {
		// Build protobuf payload.
		message := pb.Message{
			Identifier: &pb.Identifier{
				Provider: msg.Identifier.Provider,
				Subject:  msg.Identifier.Subject,
			},
			Payload:  msg.Payload,          // []byte
			Encoding: string(msg.Encoding), // e.g., "application/json"
		}

		// Marshal protobuf.
		// Use MarshalAppend to reuse capacity and avoid an extra alloc.
		size := proto.Size(&message)
		buf := make([]byte, 0, size)
		b, err := proto.MarshalOptions{}.MarshalAppend(buf, &message)
		if err != nil {
			fmt.Printf("proto marshal error: %v\n", err)
			continue
		}

		// Fixed 4-byte big-endian length prefix.
		var hdr [4]byte
		if len(b) > int(^uint32(0)) {
			fmt.Printf("message too large: %d bytes\n", len(b))
			continue
		}
		binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))

		// Write frame: [len][bytes].
		if _, err := writer.Write(hdr[:]); err != nil {
			if err == io.EOF {
				return
			}
			fmt.Printf("write len error: %v\n", err)
			return
		}
		if _, err := writer.Write(b); err != nil {
			if err == io.EOF {
				return
			}
			fmt.Printf("write body error: %v\n", err)
			return
		}

		batch++
		if batch >= flushEvery {
			if err := writer.Flush(); err != nil {
				fmt.Printf("flush error: %v\n", err)
				return
			}
			batch = 0
		}
	}

	// Final flush when channel closes.
	if err := writer.Flush(); err != nil {
		fmt.Printf("final flush error: %v\n", err)
	}
}
