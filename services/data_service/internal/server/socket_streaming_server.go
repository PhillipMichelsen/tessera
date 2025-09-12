package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
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
		_ = tc.SetNoDelay(true)                // low latency
		_ = tc.SetWriteBuffer(2 * 1024 * 1024) // bigger kernel sndbuf
		_ = tc.SetReadBuffer(256 * 1024)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
		// Note: avoid SetLinger>0; default is fine.
	}

	reader := bufio.NewReaderSize(conn, 64*1024)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		fmt.Printf("read stream UUID error: %v\n", err)
		_, _ = fmt.Fprint(conn, "Failed to read stream UUID\n")
		return
	}
	streamUUID, err := uuid.Parse(string(trimLineEnding(line)))
	if err != nil {
		_, _ = fmt.Fprint(conn, "Invalid stream UUID\n")
		return
	}

	// Give the socket server room before router drops. Make out chan larger.
	// Tune per your pressure. (in=256, out=8192 as example)
	_, out, err := s.manager.AttachClient(streamUUID, 256, 8192)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "Failed to attach to stream: %v\n", err)
		return
	}
	defer func() { _ = s.manager.DetachClient(streamUUID) }()

	// Large bufio writer to reduce syscalls.
	writer := bufio.NewWriterSize(conn, 1*1024*1024)
	defer func() {
		if err := writer.Flush(); err != nil {
			fmt.Printf("final flush error: %v\n", err)
		}
	}()

	// ---- Throughput optimizations ----
	const (
		maxBatchMsgs  = 128                  // cap number of msgs per batch
		maxBatchBytes = 1 * 1024 * 1024      // cap bytes per batch
		idleFlush     = 2 * time.Millisecond // small idle flush timer
	)
	var (
		hdr        [4]byte
		batchBuf   = &bytes.Buffer{}
		bufPool    = sync.Pool{New: func() any { return make([]byte, 64*1024) }}
		timer      = time.NewTimer(idleFlush)
		timerAlive = true
	)

	stopTimer := func() {
		if timerAlive && timer.Stop() {
			// drain if fired
			select {
			case <-timer.C:
			default:
			}
		}
		timerAlive = false
	}
	resetTimer := func() {
		if !timerAlive {
			timer.Reset(idleFlush)
			timerAlive = true
		} else {
			// re-arm
			stopTimer()
			timer.Reset(idleFlush)
			timerAlive = true
		}
	}

	// Main loop: drain out channel into a single write.
	for {
		// Block for at least one message or close.
		msg, ok := <-out
		if !ok {
			_ = writer.Flush()
			return
		}

		batchBuf.Reset()
		bytesInBatch := 0
		msgsInBatch := 0

		// Start with the message we just popped.
		{
			m := pb.Message{
				Identifier: &pb.Identifier{Key: msg.Identifier.Key()},
				Payload:    msg.Payload,
			}

			// Use pooled scratch to avoid per-message allocs in Marshal.
			scratch := bufPool.Get().([]byte)[:0]
			b, err := proto.MarshalOptions{}.MarshalAppend(scratch, &m)
			if err != nil {
				fmt.Printf("proto marshal error: %v\n", err)
				bufPool.Put(scratch[:0])
				// skip message
			} else {
				binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
				_, _ = batchBuf.Write(hdr[:])
				_, _ = batchBuf.Write(b)
				bytesInBatch += 4 + len(b)
				msgsInBatch++
				bufPool.Put(b[:0])
			}
		}

		// Opportunistically drain without blocking.
		drain := true
		resetTimer()
		for drain && msgsInBatch < maxBatchMsgs && bytesInBatch < maxBatchBytes {
			select {
			case msg, ok = <-out:
				if !ok {
					// peer closed while batching; flush what we have.
					if batchBuf.Len() > 0 {
						if _, err := writer.Write(batchBuf.Bytes()); err != nil {
							if err == io.EOF {
								return
							}
							fmt.Printf("write error: %v\n", err)
							return
						}
						if err := writer.Flush(); err != nil {
							fmt.Printf("flush error: %v\n", err)
						}
					}
					return
				}
				m := pb.Message{
					Identifier: &pb.Identifier{Key: msg.Identifier.Key()},
					Payload:    msg.Payload,
				}
				scratch := bufPool.Get().([]byte)[:0]
				b, err := proto.MarshalOptions{}.MarshalAppend(scratch, &m)
				if err != nil {
					fmt.Printf("proto marshal error: %v\n", err)
					bufPool.Put(scratch[:0])
					continue
				}
				binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
				_, _ = batchBuf.Write(hdr[:])
				_, _ = batchBuf.Write(b)
				bytesInBatch += 4 + len(b)
				msgsInBatch++
				bufPool.Put(b[:0])
			case <-timer.C:
				timerAlive = false
				// idle window hit; stop draining further this round
				drain = false
			}
		}

		// Single write for the whole batch.
		// Avoid per-message SetWriteDeadline. Let TCP handle buffering.
		if _, err := writer.Write(batchBuf.Bytes()); err != nil {
			if err == io.EOF {
				return
			}
			fmt.Printf("write error: %v\n", err)
			return
		}

		// Flush when batch is sizable or we saw the idle timer.
		// This keeps latency low without flushing every message.
		if msgsInBatch >= maxBatchMsgs ||
			bytesInBatch >= maxBatchBytes ||
			!timerAlive {
			if err := writer.Flush(); err != nil {
				fmt.Printf("flush error: %v\n", err)
				return
			}
		}
	}
}

// trimLineEnding trims a single trailing '\n' and optional '\r' before it.
func trimLineEnding(b []byte) []byte {
	n := len(b)
	if n == 0 {
		return b
	}
	if b[n-1] == '\n' {
		n--
		if n > 0 && b[n-1] == '\r' {
			n--
		}
		return b[:n]
	}
	return b
}
