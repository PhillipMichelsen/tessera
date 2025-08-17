package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "gitlab.michelsen.id/phillmichelsen/tessera/pkg/pb/data_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
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

func prettyOrRaw(b []byte, pretty bool) string {
	if !pretty || len(b) == 0 {
		return string(b)
	}
	var tmp any
	if err := json.Unmarshal(b, &tmp); err != nil {
		return string(b)
	}
	out, err := json.MarshalIndent(tmp, "", "  ")
	if err != nil {
		return string(b)
	}
	return string(out)
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

func main() {
	var ids idsFlag
	var ctlAddr string
	var strAddr string
	var pretty bool
	var timeout time.Duration

	flag.Var(&ids, "id", "identifier (provider:subject or canonical key); repeatable")
	flag.StringVar(&ctlAddr, "ctl", "127.0.0.1:50051", "gRPC control address")
	flag.StringVar(&strAddr, "str", "127.0.0.1:50052", "gRPC streaming address")
	flag.BoolVar(&pretty, "pretty", true, "pretty-print JSON payloads when possible")
	flag.DurationVar(&timeout, "timeout", 10*time.Second, "start/config/connect timeout")
	flag.Parse()

	if len(ids) == 0 {
		fmt.Fprintln(os.Stderr, "provide at least one --id (provider:subject or canonical key)")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ccCtl, err := grpc.NewClient(
		ctlAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new control client: %v\n", err)
		os.Exit(1)
	}
	defer ccCtl.Close()
	ccCtl.Connect()

	ctlConnCtx, cancelCtlConn := context.WithTimeout(ctx, timeout)
	if err := waitReady(ctlConnCtx, ccCtl); err != nil {
		cancelCtlConn()
		fmt.Fprintf(os.Stderr, "connect control: %v\n", err)
		os.Exit(1)
	}
	cancelCtlConn()

	ctl := pb.NewDataServiceControlClient(ccCtl)

	ctxStart, cancelStart := context.WithTimeout(ctx, timeout)
	startResp, err := ctl.StartStream(ctxStart, &pb.StartStreamRequest{})
	cancelStart()
	if err != nil {
		fmt.Fprintf(os.Stderr, "StartStream: %v\n", err)
		os.Exit(1)
	}
	streamUUID := startResp.GetStreamUuid()
	fmt.Printf("stream: %s\n", streamUUID)

	var pbIDs []*pb.Identifier
	for _, s := range ids {
		key, err := toIdentifierKey(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad --id: %v\n", err)
			os.Exit(2)
		}
		pbIDs = append(pbIDs, &pb.Identifier{Key: key})
	}

	ctxCfg, cancelCfg := context.WithTimeout(ctx, timeout)
	_, err = ctl.ConfigureStream(ctxCfg, &pb.ConfigureStreamRequest{
		StreamUuid:  streamUUID,
		Identifiers: pbIDs,
	})
	cancelCfg()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ConfigureStream: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("configured %d identifiers\n", len(pbIDs))

	ccStr, err := grpc.NewClient(
		strAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new streaming client: %v\n", err)
		os.Exit(1)
	}
	defer ccStr.Close()
	ccStr.Connect()

	strConnCtx, cancelStrConn := context.WithTimeout(ctx, timeout)
	if err := waitReady(strConnCtx, ccStr); err != nil {
		cancelStrConn()
		fmt.Fprintf(os.Stderr, "connect streaming: %v\n", err)
		os.Exit(1)
	}
	cancelStrConn()

	str := pb.NewDataServiceStreamingClient(ccStr)

	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	stream, err := str.ConnectStream(streamCtx, &pb.ConnectStreamRequest{StreamUuid: streamUUID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ConnectStream: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("connected; streaming… (Ctrl-C to quit)")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nshutting down")
			return
		default:
			msg, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Fprintf(os.Stderr, "recv: %v\n", err)
				os.Exit(1)
			}
			id := msg.GetIdentifier()
			fmt.Printf("[%s] bytes=%d  enc=%s  t=%s\n",
				id.GetKey(), len(msg.GetPayload()), msg.GetEncoding(),
				time.Now().Format(time.RFC3339Nano),
			)
			fmt.Println(prettyOrRaw(msg.GetPayload(), pretty))
			fmt.Println("---")
		}
	}
}
