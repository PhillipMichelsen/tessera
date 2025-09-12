package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type idsFlag []string

func (i *idsFlag) String() string { return strings.Join(*i, ",") }
func (i *idsFlag) Set(v string) error {
	if v == "" {
		return nil
	}
	*i = append(*i, v)
	return nil
}

func parseIDPair(s string) (provider, subject string, err error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("want provider:subject, got %q", s)
	}
	return parts[0], parts[1], nil
}

func toIdentifierKey(input string) (string, error) {
	if strings.Contains(input, "::") {
		return input, nil
	}
	prov, subj, err := parseIDPair(input)
	if err != nil {
		return "", err
	}
	return "raw::" + strings.ToLower(prov) + "." + subj, nil
}

func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		s := conn.GetState()
		if s == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, s) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("WaitForStateChange returned without state change")
		}
	}
}

type streamStats struct {
	TotalMsgs  int64
	TotalBytes int64
	TickMsgs   int64
	TickBytes  int64
}

type stats struct {
	TotalMsgs  int64
	TotalBytes int64
	ByStream   map[string]*streamStats
}

func main() {
	var ids idsFlag
	var ctlAddr string
	var strAddr string
	var timeout time.Duration
	var refresh time.Duration

	flag.Var(&ids, "id", "identifier (provider:subject or canonical key); repeatable")
	flag.StringVar(&ctlAddr, "ctl", "127.0.0.1:50051", "gRPC control address")
	flag.StringVar(&strAddr, "str", "127.0.0.1:50060", "socket streaming address host:port")
	flag.DurationVar(&timeout, "timeout", 10*time.Second, "start/config/connect timeout")
	flag.DurationVar(&refresh, "refresh", 1*time.Second, "dashboard refresh interval")
	flag.Parse()

	if len(ids) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "provide at least one --id (provider:subject or canonical key)")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Control channel
	ccCtl, err := grpc.NewClient(
		ctlAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "new control client: %v\n", err)
		os.Exit(1)
	}
	defer ccCtl.Close()
	ccCtl.Connect()

	ctlConnCtx, cancelCtlConn := context.WithTimeout(ctx, timeout)
	if err := waitReady(ctlConnCtx, ccCtl); err != nil {
		cancelCtlConn()
		_, _ = fmt.Fprintf(os.Stderr, "connect control: %v\n", err)
		os.Exit(1)
	}
	cancelCtlConn()

	ctl := pb.NewDataServiceControlClient(ccCtl)

	// Start stream
	ctxStart, cancelStart := context.WithTimeout(ctx, timeout)
	startResp, err := ctl.StartStream(ctxStart, &pb.StartStreamRequest{})
	cancelStart()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "StartStream: %v\n", err)
		os.Exit(1)
	}
	streamUUID := startResp.GetStreamUuid()
	fmt.Printf("stream: %s\n", streamUUID)

	// Configure identifiers
	var pbIDs []*pb.Identifier
	orderedIDs := make([]string, 0, len(ids))
	for _, s := range ids {
		key, err := toIdentifierKey(s)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "bad --id: %v\n", err)
			os.Exit(2)
		}
		pbIDs = append(pbIDs, &pb.Identifier{Key: key})
		orderedIDs = append(orderedIDs, key)
	}

	ctxCfg, cancelCfg := context.WithTimeout(ctx, timeout)
	_, err = ctl.ConfigureStream(ctxCfg, &pb.ConfigureStreamRequest{
		StreamUuid:  streamUUID,
		Identifiers: pbIDs,
	})
	cancelCfg()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ConfigureStream: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("configured %d identifiers\n", len(pbIDs))

	// Socket streaming connection
	d := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", strAddr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "dial socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetWriteBuffer(512 * 1024)
		_ = tc.SetReadBuffer(512 * 1024)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	// Send the stream UUID followed by '\n' per socket server contract.
	if _, err := io.WriteString(conn, streamUUID+"\n"); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "send stream UUID: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("connected; streaming… (Ctrl-C to quit)")

	// Receiver goroutine → channel
	type msgWrap struct {
		idKey string
		size  int
		err   error
	}
	msgCh := make(chan msgWrap, 1024)

	go func() {
		defer close(msgCh)
		r := bufio.NewReaderSize(conn, 256*1024)
		var hdr [4]byte
		for {
			if err := conn.SetReadDeadline(time.Now().Add(120 * time.Second)); err != nil {
				msgCh <- msgWrap{err: err}
				return
			}

			if _, err := io.ReadFull(r, hdr[:]); err != nil {
				msgCh <- msgWrap{err: err}
				return
			}
			n := binary.BigEndian.Uint32(hdr[:])
			if n == 0 || n > 64*1024*1024 {
				msgCh <- msgWrap{err: fmt.Errorf("invalid frame length: %d", n)}
				return
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(r, buf); err != nil {
				msgCh <- msgWrap{err: err}
				return
			}

			var m pb.Message
			if err := proto.Unmarshal(buf, &m); err != nil {
				msgCh <- msgWrap{err: fmt.Errorf("unmarshal: %w", err)}
				return
			}
			id := m.GetIdentifier().GetKey()
			msgCh <- msgWrap{idKey: id, size: len(m.GetPayload())}
		}
	}()

	// Stats and dashboard
	st := &stats{ByStream: make(map[string]*streamStats)}
	seen := make(map[string]bool, len(orderedIDs))
	for _, id := range orderedIDs {
		seen[id] = true
	}
	tick := time.NewTicker(refresh)
	defer tick.Stop()

	clear := func() { fmt.Print("\033[H\033[2J") }
	header := func() {
		fmt.Printf("stream: %s   now: %s   refresh: %s\n",
			streamUUID, time.Now().Format(time.RFC3339), refresh)
		fmt.Println("--------------------------------------------------------------------------------------")
		fmt.Printf("%-56s %10s %14s %12s %16s\n", "identifier", "msgs/s", "bytes/s", "total", "total_bytes")
		fmt.Println("--------------------------------------------------------------------------------------")
	}

	printAndReset := func() {
		clear()
		header()

		var totMsgsPS, totBytesPS float64
		for _, id := range orderedIDs {
			s, ok := st.ByStream[id]
			var msgsPS, bytesPS float64
			var totMsgs, totBytes int64
			if ok {
				msgsPS = float64(atomic.SwapInt64(&s.TickMsgs, 0)) / refresh.Seconds()
				bytesPS = float64(atomic.SwapInt64(&s.TickBytes, 0)) / refresh.Seconds()
				totMsgs = atomic.LoadInt64(&s.TotalMsgs)
				totBytes = atomic.LoadInt64(&s.TotalBytes)
			}
			totMsgsPS += msgsPS
			totBytesPS += bytesPS
			fmt.Printf("%-56s %10d %14d %12d %16d\n",
				id,
				int64(math.Round(msgsPS)),
				int64(math.Round(bytesPS)),
				totMsgs,
				totBytes,
			)
		}

		fmt.Println("--------------------------------------------------------------------------------------")
		fmt.Printf("%-56s %10d %14d %12d %16d\n",
			"TOTAL",
			int64(math.Round(totMsgsPS)),
			int64(math.Round(totBytesPS)),
			atomic.LoadInt64(&st.TotalMsgs),
			atomic.LoadInt64(&st.TotalBytes),
		)
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nshutting down")
			return

		case <-tick.C:
			printAndReset()

		case mw, ok := <-msgCh:
			if !ok {
				return
			}
			if mw.err != nil {
				if ctx.Err() != nil {
					return
				}
				if ne, ok := mw.err.(net.Error); ok && ne.Timeout() {
					_, _ = fmt.Fprintln(os.Stderr, "recv timeout")
				} else if mw.err == io.EOF {
					_, _ = fmt.Fprintln(os.Stderr, "server closed stream")
				} else {
					_, _ = fmt.Fprintf(os.Stderr, "recv: %v\n", mw.err)
				}
				os.Exit(1)
			}

			if !seen[mw.idKey] {
				seen[mw.idKey] = true
				orderedIDs = append(orderedIDs, mw.idKey)
			}

			atomic.AddInt64(&st.TotalMsgs, 1)
			atomic.AddInt64(&st.TotalBytes, int64(mw.size))

			ss := st.ByStream[mw.idKey]
			if ss == nil {
				ss = &streamStats{}
				st.ByStream[mw.idKey] = ss
			}
			atomic.AddInt64(&ss.TotalMsgs, 1)
			atomic.AddInt64(&ss.TotalBytes, int64(mw.size))
			atomic.AddInt64(&ss.TickMsgs, 1)
			atomic.AddInt64(&ss.TickBytes, int64(mw.size))
		}
	}
}
