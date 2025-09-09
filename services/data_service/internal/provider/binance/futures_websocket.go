package binance

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

const (
	endpoint = "wss://stream.binance.com:9443/stream"
	cmpName  = "binance_futures_websocket"

	// I/O limits
	readLimitBytes      = 8 << 20
	writeTimeout        = 5 * time.Second
	dialTimeout         = 10 * time.Second
	reconnectMaxBackoff = 30 * time.Second
)

type wsReq struct {
	Method string   `json:"method"`
	Params []string `json:"params,omitempty"`
	ID     uint64   `json:"id"`
}

type wsAck struct {
	Result any    `json:"result"`
	ID     uint64 `json:"id"`
}

type combinedEvent struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type FuturesWebsocket struct {
	out chan<- domain.Message

	mu     sync.RWMutex
	active map[string]bool

	connMu sync.Mutex
	conn   *websocket.Conn
	cancel context.CancelFunc

	reqID   atomic.Uint64
	pending map[uint64]chan error
	pmu     sync.Mutex

	// pumps
	writer chan []byte
	once   sync.Once
	stopCh chan struct{}
}

func NewFuturesWebsocket(out chan<- domain.Message) *FuturesWebsocket {
	return &FuturesWebsocket{
		out:     out,
		active:  make(map[string]bool),
		pending: make(map[uint64]chan error),
		writer:  make(chan []byte, 256),
		stopCh:  make(chan struct{}),
	}
}

func (p *FuturesWebsocket) Start() error {
	var startErr error
	p.once.Do(func() {
		go p.run()
	})
	return startErr
}

func (p *FuturesWebsocket) Stop() {
	close(p.stopCh)
	p.connMu.Lock()
	if p.cancel != nil {
		p.cancel()
	}
	if p.conn != nil {
		_ = p.conn.Close(websocket.StatusNormalClosure, "shutdown")
		p.conn = nil
	}
	p.connMu.Unlock()

	// fail pending waiters
	p.pmu.Lock()
	for id, ch := range p.pending {
		ch <- errors.New("provider stopped")
		close(ch)
		delete(p.pending, id)
	}
	p.pmu.Unlock()

	slog.Default().Info("stopped", "cmp", cmpName)
}

func (p *FuturesWebsocket) StartStreams(keys []string) <-chan error {
	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		if len(keys) == 0 {
			ch <- nil
			return
		}
		id, ack := p.sendReq("SUBSCRIBE", keys)
		if ack == nil {
			ch <- errors.New("not connected")
			slog.Default().Error("subscribe failed; not connected", "cmp", cmpName, "keys", keys)
			return
		}
		if err := <-ack; err != nil {
			ch <- err
			slog.Default().Error("subscribe NACK", "cmp", cmpName, "id", id, "keys", keys, "err", err)
			return
		}
		p.mu.Lock()
		for _, k := range keys {
			p.active[k] = true
		}
		p.mu.Unlock()
		slog.Default().Info("subscribed", "cmp", cmpName, "id", id, "keys", keys)
		ch <- nil
	}()
	return ch
}

func (p *FuturesWebsocket) StopStreams(keys []string) <-chan error {
	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		if len(keys) == 0 {
			ch <- nil
			return
		}
		id, ack := p.sendReq("UNSUBSCRIBE", keys)
		if ack == nil {
			ch <- errors.New("not connected")
			slog.Default().Error("unsubscribe failed; not connected", "cmp", cmpName, "keys", keys)
			return
		}
		if err := <-ack; err != nil {
			ch <- err
			slog.Default().Error("unsubscribe NACK", "cmp", cmpName, "id", id, "keys", keys, "err", err)
			return
		}
		p.mu.Lock()
		for _, k := range keys {
			delete(p.active, k)
		}
		p.mu.Unlock()
		slog.Default().Info("unsubscribed", "cmp", cmpName, "id", id, "keys", keys)
		ch <- nil
	}()
	return ch
}

func (p *FuturesWebsocket) Fetch(key string) (domain.Message, error) {
	return domain.Message{}, errors.New("not implemented")
}

func (p *FuturesWebsocket) IsStreamActive(key string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active[key]
}

func (p *FuturesWebsocket) IsValidSubject(key string, _ bool) bool {
	return len(key) > 0
}

// internal

func (p *FuturesWebsocket) run() {
	backoff := time.Second

	for {
		// stop?
		select {
		case <-p.stopCh:
			return
		default:
		}

		if err := p.connect(); err != nil {
			slog.Default().Error("dial failed", "cmp", cmpName, "err", err)
			time.Sleep(backoff)
			if backoff < reconnectMaxBackoff {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		// resubscribe existing keys
		func() {
			p.mu.RLock()
			if len(p.active) > 0 {
				keys := make([]string, 0, len(p.active))
				for k := range p.active {
					keys = append(keys, k)
				}
				_, ack := p.sendReq("SUBSCRIBE", keys)
				if ack != nil {
					if err := <-ack; err != nil {
						slog.Default().Warn("resubscribe error", "cmp", cmpName, "err", err)
					} else {
						slog.Default().Info("resubscribed", "cmp", cmpName, "count", len(keys))
					}
				}
			}
			p.mu.RUnlock()
		}()

		// run read and write pumps
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 2)
		go func() { errc <- p.readLoop(ctx) }()
		go func() { errc <- p.writeLoop(ctx) }()

		// wait for failure or stop
		var err error
		select {
		case <-p.stopCh:
			cancel()
			p.cleanupConn()
			return
		case err = <-errc:
			cancel()
		}

		// fail pendings on error
		p.pmu.Lock()
		for id, ch := range p.pending {
			ch <- err
			close(ch)
			delete(p.pending, id)
		}
		p.pmu.Unlock()

		slog.Default().Error("ws loop error; reconnecting", "cmp", cmpName, "err", err)
		p.cleanupConn()
	}
}

func (p *FuturesWebsocket) connect() error {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.conn != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	c, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
		OnPingReceived: func(ctx context.Context, _ []byte) bool {
			slog.Default().Info("ping received", "cmp", cmpName)
			return true
		},
	})
	if err != nil {
		cancel()
		return err
	}

	c.SetReadLimit(8 << 20)

	p.conn = c
	p.cancel = cancel
	slog.Default().Info("connected", "cmp", cmpName, "endpoint", endpoint)
	return nil
}

func (p *FuturesWebsocket) cleanupConn() {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	if p.conn != nil {
		_ = p.conn.Close(websocket.StatusAbnormalClosure, "reconnect")
		p.conn = nil
	}
}

func (p *FuturesWebsocket) writeLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case b := <-p.writer:
			p.connMu.Lock()
			c := p.conn
			p.connMu.Unlock()
			if c == nil {
				return errors.New("conn nil")
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func (p *FuturesWebsocket) readLoop(ctx context.Context) error {
	slog.Default().Info("read loop started", "cmp", cmpName)
	defer slog.Default().Info("read loop exited", "cmp", cmpName)

	for {
		p.connMu.Lock()
		c := p.conn
		p.connMu.Unlock()
		if c == nil {
			return errors.New("conn nil")
		}

		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}

		// ACK
		var ack wsAck
		if json.Unmarshal(data, &ack) == nil && ack.ID != 0 {
			p.pmu.Lock()
			if ch, ok := p.pending[ack.ID]; ok {
				if ack.Result == nil {
					ch <- nil
					slog.Default().Debug("ack ok", "cmp", cmpName, "id", ack.ID)
				} else {
					resb, _ := json.Marshal(ack.Result)
					ch <- errors.New(string(resb))
					slog.Default().Warn("ack error", "cmp", cmpName, "id", ack.ID, "result", string(resb))
				}
				close(ch)
				delete(p.pending, ack.ID)
			} else {
				slog.Default().Warn("ack with unknown id", "cmp", cmpName, "id", ack.ID)
			}
			p.pmu.Unlock()
			continue
		}

		// Combined stream payload
		var evt combinedEvent
		if json.Unmarshal(data, &evt) == nil && evt.Stream != "" {
			ident, _ := domain.RawID(cmpName, evt.Stream)
			msg := domain.Message{
				Identifier: ident,
				Payload:    evt.Data,
			}
			select {
			case p.out <- msg:
			default:
				slog.Default().Warn("dropping message since router buffer full", "cmp", cmpName, "stream", evt.Stream)
			}
			continue
		}

		// Unknown frame
		const maxSample = 512
		if len(data) > maxSample {
			slog.Default().Debug("unparsed frame", "cmp", cmpName, "size", len(data))
		} else {
			slog.Default().Debug("unparsed frame", "cmp", cmpName, "size", len(data), "body", string(data))
		}
	}
}

func (p *FuturesWebsocket) sendReq(method string, params []string) (uint64, <-chan error) {
	p.connMu.Lock()
	c := p.conn
	p.connMu.Unlock()
	if c == nil {
		return 0, nil
	}

	id := p.reqID.Add(1)
	req := wsReq{Method: method, Params: params, ID: id}
	b, _ := json.Marshal(req)

	ack := make(chan error, 1)
	p.pmu.Lock()
	p.pending[id] = ack
	p.pmu.Unlock()

	// enqueue to single writer to avoid concurrent writes
	select {
	case p.writer <- b:
	default:
		// avoid blocking the caller; offload
		go func() { p.writer <- b }()
	}

	slog.Default().Debug("request enqueued", "cmp", cmpName, "id", id, "method", method, "params", params)
	return id, ack
}
