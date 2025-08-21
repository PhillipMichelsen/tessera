package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

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

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetWriteBuffer(512 * 1024)
		_ = tc.SetReadBuffer(512 * 1024)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	reader := bufio.NewReader(conn)

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

	outCh, err := s.manager.ConnectClientStream(streamUUID)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "Failed to connect to stream: %v\n", err)
		return
	}
	defer s.manager.DisconnectClientStream(streamUUID)

	writer := bufio.NewWriterSize(conn, 256*1024)
	defer func(w *bufio.Writer) {
		if err := w.Flush(); err != nil {
			fmt.Printf("final flush error: %v\n", err)
		}
	}(writer)

	const flushEvery = 32
	batch := 0

	for msg := range outCh {
		m := pb.Message{
			Identifier: &pb.Identifier{Key: msg.Identifier.Key()},
			Payload:    msg.Payload,
			Encoding:   string(msg.Encoding),
		}

		size := proto.Size(&m)
		buf := make([]byte, 0, size)
		b, err := proto.MarshalOptions{}.MarshalAppend(buf, &m)
		if err != nil {
			fmt.Printf("proto marshal error: %v\n", err)
			continue
		}

		var hdr [4]byte
		if len(b) > int(^uint32(0)) {
			fmt.Printf("message too large: %d bytes\n", len(b))
			continue
		}
		binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))

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

	if err := writer.Flush(); err != nil {
		fmt.Printf("final flush error: %v\n", err)
	}
}
