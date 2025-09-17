package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type opType uint8

const (
	opSubscribe opType = iota + 1
	opUnsubscribe
)

type pendingBatch struct {
	Op       opType
	Subjects []string
	Waiters  map[string][]chan error
}

type shard struct {
	ID  uuid.UUID
	url string
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc

	conn *websocket.Conn

	mu     sync.RWMutex
	active map[string]struct{}

	subBatch   map[string][]chan error
	unsubBatch map[string][]chan error

	sendQ      chan []byte
	rateTicker *time.Ticker
	pingTicker *time.Ticker

	pendingMu   sync.Mutex
	pendingByID map[uint64]*pendingBatch
	nextReqID   func() uint64

	wg  sync.WaitGroup
	bus chan<- domain.Message
}

func newShard(pctx context.Context, cfg Config, bus chan<- domain.Message, next func() uint64) (*shard, error) {
	id := uuid.New()
	ctx, cancel := context.WithCancel(pctx)
	sh := &shard{
		ID:          id,
		url:         cfg.Endpoint,
		cfg:         cfg,
		ctx:         ctx,
		cancel:      cancel,
		active:      make(map[string]struct{}),
		subBatch:    make(map[string][]chan error),
		unsubBatch:  make(map[string][]chan error),
		sendQ:       make(chan []byte, 256),
		pendingByID: make(map[uint64]*pendingBatch),
		nextReqID:   next,
		bus:         bus,
	}

	// per-shard rate limiter; also drives batch flushing
	rate := cfg.RateLimitPerSec
	if rate <= 0 {
		rate = 1
	}
	interval := time.Second / time.Duration(rate)
	sh.rateTicker = time.NewTicker(interval)
	sh.pingTicker = time.NewTicker(30 * time.Second)

	slog.Default().Info("shard created", "cmp", providerName, "shard", sh.ID.String())

	if err := sh.connect(); err != nil {
		slog.Default().Error("shard connection failed", "cmp", providerName, "shard", sh.ID.String(), "error", err)
		return nil, err
	}
	sh.startLoops()
	return sh, nil
}

func (s *shard) connect() error {
	dctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(dctx, s.url, &websocket.DialOptions{})
	if err != nil {
		slog.Default().Error("shard connection error", "cmp", providerName, "shard", s.ID.String(), "error", err)
		return err
	}
	s.conn = c
	slog.Default().Info("shard connected", "cmp", providerName, "shard", s.ID.String())
	return nil
}

func (s *shard) startLoops() {
	s.wg.Add(3)
	go s.writeLoop()
	go s.readLoop()
	go s.pingLoop()
}

func (s *shard) stop() {
	s.cancel()
	if s.conn != nil {
		_ = s.conn.Close(websocket.StatusNormalClosure, "shutdown")
	}
	if s.rateTicker != nil {
		s.rateTicker.Stop()
	}
	if s.pingTicker != nil {
		s.pingTicker.Stop()
	}
	s.wg.Wait()

	s.pendingMu.Lock()
	for _, p := range s.pendingByID {
		for _, arr := range p.Waiters {
			for _, ch := range arr {
				select {
				case ch <- context.Canceled:
				default:
				}
			}
		}
	}
	s.pendingByID = map[uint64]*pendingBatch{}
	s.pendingMu.Unlock()

	s.mu.Lock()
	for _, arr := range s.subBatch {
		for _, ch := range arr {
			select {
			case ch <- context.Canceled:
			default:
			}
		}
	}
	for _, arr := range s.unsubBatch {
		for _, ch := range arr {
			select {
			case ch <- context.Canceled:
			default:
			}
		}
	}
	s.subBatch = map[string][]chan error{}
	s.unsubBatch = map[string][]chan error{}
	s.mu.Unlock()

	slog.Default().Info("shard stopped", "cmp", providerName, "shard", s.ID.String())
}

func (s *shard) enqueueSubscribe(subject string, ch chan error) {
	s.mu.Lock()
	s.subBatch[subject] = append(s.subBatch[subject], ch)
	s.mu.Unlock()
	slog.Default().Debug("shard enqueue subscribe", "cmp", providerName, "shard", s.ID, "subject", subject)
}

func (s *shard) enqueueUnsubscribe(subject string, ch chan error) {
	s.mu.Lock()
	s.unsubBatch[subject] = append(s.unsubBatch[subject], ch)
	s.mu.Unlock()
	slog.Default().Debug("shard enqueue unsubscribe", "cmp", providerName, "shard", s.ID, "subject", subject)
}

func (s *shard) isActive(subj string) bool {
	s.mu.RLock()
	_, ok := s.active[subj]
	s.mu.RUnlock()
	return ok
}

func (s *shard) activeCount() int {
	s.mu.RLock()
	n := len(s.active)
	s.mu.RUnlock()
	return n
}

func (s *shard) loadEstimate() int { // active + pending subscribes
	s.mu.RLock()
	n := len(s.active) + len(s.subBatch)
	s.mu.RUnlock()
	return n
}

func (s *shard) isIdle() bool {
	s.mu.RLock()
	idle := len(s.active) == 0 && len(s.subBatch) == 0 && len(s.unsubBatch) == 0
	s.mu.RUnlock()
	return idle
}

func (s *shard) activeList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.active))
	for k := range s.active {
		out = append(out, k)
	}
	return out
}

func (s *shard) writeLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.rateTicker.C:
			// snapshot and clear pending batch operations
			var subs, unsubs map[string][]chan error
			s.mu.Lock()
			if len(s.subBatch) > 0 {
				subs = s.subBatch
				s.subBatch = make(map[string][]chan error)
			}
			if len(s.unsubBatch) > 0 {
				unsubs = s.unsubBatch
				s.unsubBatch = make(map[string][]chan error)
			}
			s.mu.Unlock()

			// send SUBSCRIBE batch
			if len(subs) > 0 {
				params := make([]string, 0, len(subs))
				waiters := make(map[string][]chan error, len(subs))
				for k, v := range subs {
					params = append(params, k)
					waiters[k] = v
				}
				id := s.nextReqID()
				frame := map[string]any{"method": "SUBSCRIBE", "params": params, "id": id}
				payload, _ := json.Marshal(frame)
				s.recordPending(id, opSubscribe, params, waiters)
				if err := s.writeFrame(payload); err != nil {
					s.reconnect()
					return
				}
			}

			// send UNSUBSCRIBE batch
			if len(unsubs) > 0 {
				params := make([]string, 0, len(unsubs))
				waiters := make(map[string][]chan error, len(unsubs))
				for k, v := range unsubs {
					params = append(params, k)
					waiters[k] = v
				}
				id := s.nextReqID()
				frame := map[string]any{"method": "UNSUBSCRIBE", "params": params, "id": id}
				payload, _ := json.Marshal(frame)
				s.recordPending(id, opUnsubscribe, params, waiters)
				if err := s.writeFrame(payload); err != nil {
					s.reconnect()
					return
				}
			}

			// optional: one queued ad-hoc frame per tick
			select {
			case msg := <-s.sendQ:
				if err := s.writeFrame(msg); err != nil {
					s.reconnect()
					return
				}
			default:
			}
		}
	}
}

func (s *shard) writeFrame(msg []byte) error {
	wctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	err := s.conn.Write(wctx, websocket.MessageText, msg)
	if err != nil {
		slog.Default().Warn("shard write error", "cmp", providerName, "shard", s.ID, "error", err)
	}
	return err
}

func (s *shard) readLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			// longer idle timeout when no active subscriptions
			timeout := 60 * time.Second
			if s.activeCount() == 0 {
				timeout = 5 * time.Minute
			}
			rctx, cancel := context.WithTimeout(s.ctx, timeout)
			_, data, err := s.conn.Read(rctx)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					slog.Default().Debug("shard read idle timeout", "cmp", providerName, "shard", s.ID)
					continue
				}
				slog.Default().Warn("shard read error", "cmp", providerName, "shard", s.ID, "error", err)
				s.reconnect()
				return
			}

			if bytes.Contains(data, []byte("\"id\"")) {
				var ack struct {
					ID     uint64           `json:"id"`
					Result *json.RawMessage `json:"result"`
					Error  *struct {
						Code int    `json:"code"`
						Msg  string `json:"msg"`
					} `json:"error"`
				}
				if json.Unmarshal(data, &ack) == nil && ack.ID != 0 {
					if ack.Error != nil {
						slog.Default().Warn("shard ack error", "cmp", providerName, "shard", s.ID, "id", ack.ID, "code", ack.Error.Code, "msg", ack.Error.Msg)
						s.resolvePending(ack.ID, fmt.Errorf("binance error %d: %s", ack.Error.Code, ack.Error.Msg))
					} else {
						slog.Default().Debug("shard ack ok", "cmp", providerName, "shard", s.ID, "id", ack.ID)
						s.resolvePending(ack.ID, nil)
					}
					continue
				}
			}

			var frame struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}
			if json.Unmarshal(data, &frame) == nil && frame.Stream != "" {
				id, err := domain.RawID(providerName, frame.Stream)
				if err == nil {
					select {
					case s.bus <- domain.Message{Identifier: id, Payload: frame.Data}:
					default:
					}
				}
				continue
			}
			slog.Default().Debug("shard unknown message", "cmp", providerName, "shard", s.ID, "data", string(data))
		}
	}
}

func (s *shard) pingLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.pingTicker.C:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			err := s.conn.Ping(ctx)
			cancel()
			if err != nil {
				slog.Default().Warn("shard ping failed", "cmp", providerName, "shard", s.ID, "error", err)
				s.reconnect()
				return
			}
		}
	}
}

func (s *shard) recordPending(id uint64, op opType, subjects []string, waiters map[string][]chan error) {
	s.pendingMu.Lock()
	s.pendingByID[id] = &pendingBatch{Op: op, Subjects: subjects, Waiters: waiters}
	s.pendingMu.Unlock()
}

func (s *shard) resolvePending(id uint64, err error) {
	s.pendingMu.Lock()
	p := s.pendingByID[id]
	delete(s.pendingByID, id)
	s.pendingMu.Unlock()
	if p == nil {
		return
	}

	if err == nil {
		s.mu.Lock()
		if p.Op == opSubscribe {
			for _, subj := range p.Subjects {
				s.active[subj] = struct{}{}
			}
			slog.Default().Debug("shard subscribed", "cmp", providerName, "shard", s.ID, "subjects", p.Subjects)
		} else {
			for _, subj := range p.Subjects {
				delete(s.active, subj)
			}
			slog.Default().Debug("shard unsubscribed", "cmp", providerName, "shard", s.ID, "subjects", p.Subjects)
		}
		s.mu.Unlock()
	} else {
		slog.Default().Warn("shard pending error", "cmp", providerName, "shard", s.ID, "error", err)
	}

	for _, arr := range p.Waiters {
		for _, ch := range arr {
			select {
			case ch <- err:
			default:
			}
		}
	}
}

func (s *shard) queue(payload []byte) {
	select {
	case s.sendQ <- payload:
	default:
		slog.Default().Warn("shard sendQ full, dropping one message", "cmp", providerName, "shard", s.ID)
		<-s.sendQ
		s.sendQ <- payload
	}
}

func (s *shard) reconnect() {
	reconnectStartTime := time.Now()
	if s.conn != nil {
		_ = s.conn.Close(websocket.StatusGoingAway, "reconnect")
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			if err := s.connect(); err != nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			// re-stage current actives for batch subscribe on next tick
			s.mu.RLock()
			for k := range s.active {
				s.subBatch[k] = append(s.subBatch[k], nil)
			}
			s.mu.RUnlock()

			// restart loops
			s.startLoops()
			slog.Default().Info("shard reconnected", "cmp", providerName, "shard", s.ID, "downtime", time.Since(reconnectStartTime).String())
			return
		}
	}
}
