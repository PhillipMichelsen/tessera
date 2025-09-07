package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

const (
	wsURL              = "wss://fstream.binance.com/stream"
	writeRatePerSecond = 8               // hard cap per second
	writeBurst         = 8               // token bucket burst
	writeWait          = 5 * time.Second // per write deadline

	batchPeriod = 1 * time.Second // batch SUB/UNSUB every second

	reconnectMin = 500 * time.Millisecond
	reconnectMax = 10 * time.Second
)

// internal stream states (provider stays simple; manager relies on IsStreamActive)
type streamState uint8

const (
	stateUnknown streamState = iota
	statePendingSub
	stateActive
	statePendingUnsub
	stateInactive
	stateError
)

type FuturesWebsocket struct {
	dial websocket.Dialer
	hdr  http.Header

	// desired subscriptions and sinks
	mu      sync.Mutex
	desired map[string]bool                  // subject -> want subscribed
	sinks   map[string]chan<- domain.Message // subject -> destination
	states  map[string]streamState           // subject -> state

	// waiters per subject
	startWaiters map[string][]chan error
	stopWaiters  map[string][]chan error

	// batching queues
	subQ   chan string
	unsubQ chan string

	// websocket
	writeMu sync.Mutex
	conn    *websocket.Conn

	// rate limit tokens
	tokensCh chan struct{}
	stopRate chan struct{}

	// lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup

	// ack tracking
	ackMu    sync.Mutex
	idSeq    uint64
	pendingA map[int64]ackBatch
}

type ackBatch struct {
	method   string // "SUBSCRIBE" or "UNSUBSCRIBE"
	subjects []string
}

func NewFuturesWebsocket() *FuturesWebsocket {
	return &FuturesWebsocket{
		desired:      make(map[string]bool),
		sinks:        make(map[string]chan<- domain.Message),
		states:       make(map[string]streamState),
		startWaiters: make(map[string][]chan error),
		stopWaiters:  make(map[string][]chan error),
		subQ:         make(chan string, 4096),
		unsubQ:       make(chan string, 4096),
		tokensCh:     make(chan struct{}, writeBurst),
		stopRate:     make(chan struct{}),
		stopCh:       make(chan struct{}),
		pendingA:     make(map[int64]ackBatch),
	}
}

/* provider.Provider */

func (b *FuturesWebsocket) Start() error {
	// token bucket
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		t := time.NewTicker(time.Second / writeRatePerSecond)
		defer t.Stop()
		// prime burst
		for i := 0; i < writeBurst; i++ {
			select {
			case b.tokensCh <- struct{}{}:
			default:
			}
		}
		for {
			select {
			case <-b.stopRate:
				return
			case <-t.C:
				select {
				case b.tokensCh <- struct{}{}:
				default:
				}
			}
		}
	}()

	// connection manager
	b.wg.Add(1)
	go b.run()

	// batcher
	b.wg.Add(1)
	go b.batcher()

	return nil
}

func (b *FuturesWebsocket) Stop() {
	close(b.stopCh)
	close(b.stopRate)

	b.writeMu.Lock()
	if b.conn != nil {
		_ = b.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
		_ = b.conn.Close()
		b.conn = nil
	}
	b.writeMu.Unlock()

	b.wg.Wait()

	// resolve any remaining waiters with an error
	b.mu.Lock()
	defer b.mu.Unlock()
	for subj, ws := range b.startWaiters {
		for _, ch := range ws {
			select {
			case ch <- errors.New("provider stopped"):
			default:
			}
			close(ch)
		}
		delete(b.startWaiters, subj)
	}
	for subj, ws := range b.stopWaiters {
		for _, ch := range ws {
			select {
			case ch <- errors.New("provider stopped"):
			default:
			}
			close(ch)
		}
		delete(b.stopWaiters, subj)
	}
}

func (b *FuturesWebsocket) StartStream(subject string, dst chan<- domain.Message) <-chan error {
	fmt.Println("Starting stream for subject:", subject)
	ch := make(chan error, 1)

	if subject == "" {
		ch <- fmt.Errorf("empty subject")
		close(ch)
		return ch
	}

	b.mu.Lock()
	// mark desired, update sink
	b.desired[subject] = true
	b.sinks[subject] = dst

	// fast path: already active
	if b.states[subject] == stateActive {
		b.mu.Unlock()
		ch <- nil
		close(ch)
		return ch
	}

	// enqueue waiter and transition if needed
	b.startWaiters[subject] = append(b.startWaiters[subject], ch)
	if b.states[subject] != statePendingSub {
		b.states[subject] = statePendingSub
		select {
		case b.subQ <- subject:
		default:
			// queue full → fail fast
			ws := b.startWaiters[subject]
			delete(b.startWaiters, subject)
			b.states[subject] = stateError
			b.mu.Unlock()
			for _, w := range ws {
				w <- fmt.Errorf("subscribe queue full")
				close(w)
			}
			return ch
		}
	}
	b.mu.Unlock()
	return ch
}

func (b *FuturesWebsocket) StopStream(subject string) <-chan error {
	fmt.Println("Stopping stream for subject:", subject)
	ch := make(chan error, 1)

	if subject == "" {
		ch <- fmt.Errorf("empty subject")
		close(ch)
		return ch
	}

	b.mu.Lock()
	// mark no longer desired; keep sink until UNSUB ack to avoid drops
	b.desired[subject] = false

	// already inactive
	if b.states[subject] == stateInactive {
		b.mu.Unlock()
		ch <- nil
		close(ch)
		return ch
	}

	// enqueue waiter and transition if needed
	b.stopWaiters[subject] = append(b.stopWaiters[subject], ch)
	if b.states[subject] != statePendingUnsub {
		b.states[subject] = statePendingUnsub
		select {
		case b.unsubQ <- subject:
		default:
			// queue full → fail fast
			ws := b.stopWaiters[subject]
			delete(b.stopWaiters, subject)
			b.states[subject] = stateError
			b.mu.Unlock()
			for _, w := range ws {
				w <- fmt.Errorf("unsubscribe queue full")
				close(w)
			}
			return ch
		}
	}
	b.mu.Unlock()
	return ch
}

func (b *FuturesWebsocket) Fetch(_ string) (domain.Message, error) {
	return domain.Message{}, fmt.Errorf("fetch not supported")
}

func (b *FuturesWebsocket) IsStreamActive(subject string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.states[subject] == stateActive
}

func (b *FuturesWebsocket) IsValidSubject(subject string, isFetch bool) bool {
	return !isFetch && subject != ""
}

/* internals */

func (b *FuturesWebsocket) run() {
	defer b.wg.Done()

	backoff := reconnectMin

	dial := func() (*websocket.Conn, error) {
		c, _, err := b.dial.Dial(wsURL, b.hdr)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		c, err := dial()
		if err != nil {
			time.Sleep(backoff)
			backoff = minDur(backoff*2, reconnectMax)
			continue
		}
		backoff = reconnectMin

		b.writeMu.Lock()
		b.conn = c
		b.writeMu.Unlock()

		// Resubscribe desired subjects in one batched SUB.
		want := b.snapshotDesired(true) // only desired==true
		if len(want) > 0 {
			_ = b.sendSubscribe(want)
			b.mu.Lock()
			for _, s := range want {
				if b.states[s] != stateActive {
					b.states[s] = statePendingSub
				}
			}
			b.mu.Unlock()
		}

		err = b.readLoop(c)

		// tear down connection
		b.writeMu.Lock()
		if b.conn != nil {
			_ = b.conn.Close()
			b.conn = nil
		}
		b.writeMu.Unlock()

		select {
		case <-b.stopCh:
			return
		default:
			time.Sleep(backoff)
			backoff = minDur(backoff*2, reconnectMax)
		}
	}
}

func (b *FuturesWebsocket) batcher() {
	defer b.wg.Done()

	t := time.NewTicker(batchPeriod)
	defer t.Stop()

	var subs, unsubs []string

	flush := func() {
		if len(subs) > 0 {
			_ = b.sendSubscribe(subs)
			subs = subs[:0]
		}
		if len(unsubs) > 0 {
			_ = b.sendUnsubscribe(unsubs)
			unsubs = unsubs[:0]
		}
	}

	for {
		select {
		case <-b.stopCh:
			return
		case s := <-b.subQ:
			if s != "" {
				subs = append(subs, s)
			}
		case s := <-b.unsubQ:
			if s != "" {
				unsubs = append(unsubs, s)
			}
		case <-t.C:
			flush()
		}
	}
}

func (b *FuturesWebsocket) readLoop(c *websocket.Conn) error {
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}

		fmt.Println("Received message:", string(raw))

		// Stream data or command ack
		if hasField(raw, `"stream"`) {
			var container struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(raw, &container); err != nil || container.Stream == "" {
				continue
			}

			b.mu.Lock()
			dst, ok := b.sinks[container.Stream]
			st := b.states[container.Stream]
			b.mu.Unlock()

			if !ok || st == stateInactive || st == statePendingUnsub {
				continue
			}

			id, err := domain.RawID("binance_futures_websocket", container.Stream)
			if err != nil {
				continue
			}
			msg := domain.Message{
				Identifier: id,
				Payload:    container.Data,
				Encoding:   domain.EncodingJSON,
			}

			select {
			case dst <- msg:
			default:
				// drop on backpressure
			}
			continue
		}

		// Ack path
		var ack struct {
			Result json.RawMessage `json:"result"`
			ID     int64           `json:"id"`
		}
		if err := json.Unmarshal(raw, &ack); err != nil || ack.ID == 0 {
			continue
		}

		b.ackMu.Lock()
		batch, ok := b.pendingA[ack.ID]
		if ok {
			delete(b.pendingA, ack.ID)
		}
		b.ackMu.Unlock()
		if !ok {
			continue
		}

		ackErr := (len(ack.Result) > 0 && string(ack.Result) != "null")

		switch batch.method {
		case "SUBSCRIBE":
			b.mu.Lock()
			for _, s := range batch.subjects {
				if ackErr {
					b.states[s] = stateError
					// fail all start waiters
					ws := b.startWaiters[s]
					delete(b.startWaiters, s)
					b.mu.Unlock()
					for _, ch := range ws {
						ch <- fmt.Errorf("subscribe failed")
						close(ch)
					}
					b.mu.Lock()
					continue
				}
				// success
				b.states[s] = stateActive
				ws := b.startWaiters[s]
				delete(b.startWaiters, s)
				dst := b.sinks[s]
				b.mu.Unlock()

				for _, ch := range ws {
					ch <- nil
					close(ch)
				}
				_ = dst // messages will flow via readLoop
				b.mu.Lock()
			}
			b.mu.Unlock()

		case "UNSUBSCRIBE":
			b.mu.Lock()
			for _, s := range batch.subjects {
				if ackErr {
					b.states[s] = stateError
					ws := b.stopWaiters[s]
					delete(b.stopWaiters, s)
					b.mu.Unlock()
					for _, ch := range ws {
						ch <- fmt.Errorf("unsubscribe failed")
						close(ch)
					}
					b.mu.Lock()
					continue
				}
				// success
				b.states[s] = stateInactive
				delete(b.sinks, s) // stop delivering
				ws := b.stopWaiters[s]
				delete(b.stopWaiters, s)
				b.mu.Unlock()
				for _, ch := range ws {
					ch <- nil
					close(ch)
				}
				b.mu.Lock()
			}
			b.mu.Unlock()
		}
	}
}

func (b *FuturesWebsocket) nextID() int64 {
	return int64(atomic.AddUint64(&b.idSeq, 1))
}

func (b *FuturesWebsocket) sendSubscribe(subjects []string) error {
	if len(subjects) == 0 {
		return nil
	}
	id := b.nextID()
	req := map[string]any{
		"method": "SUBSCRIBE",
		"params": subjects,
		"id":     id,
	}
	if err := b.writeJSON(req); err != nil {
		// mark error and fail waiters
		b.mu.Lock()
		for _, s := range subjects {
			b.states[s] = stateError
			ws := b.startWaiters[s]
			delete(b.startWaiters, s)
			b.mu.Unlock()
			for _, ch := range ws {
				ch <- fmt.Errorf("subscribe send failed")
				close(ch)
			}
			b.mu.Lock()
		}
		b.mu.Unlock()
		return err
	}
	b.ackMu.Lock()
	b.pendingA[id] = ackBatch{method: "SUBSCRIBE", subjects: append([]string(nil), subjects...)}
	b.ackMu.Unlock()
	return nil
}

func (b *FuturesWebsocket) sendUnsubscribe(subjects []string) error {
	if len(subjects) == 0 {
		return nil
	}
	id := b.nextID()
	req := map[string]any{
		"method": "UNSUBSCRIBE",
		"params": subjects,
		"id":     id,
	}
	if err := b.writeJSON(req); err != nil {
		b.mu.Lock()
		for _, s := range subjects {
			b.states[s] = stateError
			ws := b.stopWaiters[s]
			delete(b.stopWaiters, s)
			b.mu.Unlock()
			for _, ch := range ws {
				ch <- fmt.Errorf("unsubscribe send failed")
				close(ch)
			}
			b.mu.Lock()
		}
		b.mu.Unlock()
		return err
	}
	b.ackMu.Lock()
	b.pendingA[id] = ackBatch{method: "UNSUBSCRIBE", subjects: append([]string(nil), subjects...)}
	b.ackMu.Unlock()
	return nil
}

func (b *FuturesWebsocket) writeJSON(v any) error {
	// token bucket
	select {
	case <-b.stopCh:
		return fmt.Errorf("stopped")
	case <-b.tokensCh:
	}

	b.writeMu.Lock()
	c := b.conn
	b.writeMu.Unlock()
	if c == nil {
		return fmt.Errorf("not connected")
	}

	_ = c.SetWriteDeadline(time.Now().Add(writeWait))
	return c.WriteJSON(v)
}

/* utilities */

func (b *FuturesWebsocket) snapshotDesired(onlyTrue bool) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for s, want := range b.desired {
		if !onlyTrue || want {
			out = append(out, s)
		}
	}
	return out
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func hasField(raw []byte, needle string) bool {
	// cheap check; avoids another allocation if it's obviously an ACK
	return json.Valid(raw) && byteContains(raw, needle)
}

func byteContains(b []byte, sub string) bool {
	n := len(sub)
	if n == 0 || len(b) < n {
		return false
	}
	// naive search; sufficient for small frames
	for i := 0; i <= len(b)-n; i++ {
		if string(b[i:i+n]) == sub {
			return true
		}
	}
	return false
}
