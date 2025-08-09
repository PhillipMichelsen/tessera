package binance

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type FuturesWebsocket struct {
	conn          *websocket.Conn
	activeStreams map[string]chan domain.Message
	mu            sync.Mutex
}

func NewFuturesWebsocket() *FuturesWebsocket {
	return &FuturesWebsocket{
		activeStreams: make(map[string]chan domain.Message),
	}
}

func (b *FuturesWebsocket) Start() error {
	c, _, err := websocket.DefaultDialer.Dial("wss://fstream.binance.com/stream", nil)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	b.conn = c
	go b.readLoop()
	return nil
}

func (b *FuturesWebsocket) Stop() {
	if b.conn != nil {
		err := b.conn.Close()
		if err != nil {
			panic(fmt.Errorf("failed to close websocket connection: %w", err))
		}
	}
}

func (b *FuturesWebsocket) RequestStream(subject string, ch chan domain.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.activeStreams[subject]; ok {
		return nil
	}

	msg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": []string{subject},
		"id":     len(b.activeStreams) + 1,
	}
	if err := b.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	b.activeStreams[subject] = ch
	fmt.Println("Subscribed to stream:", subject)
	return nil
}

func (b *FuturesWebsocket) CancelStream(subject string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.activeStreams[subject]; !ok {
		return
	}

	msg := map[string]interface{}{
		"method": "UNSUBSCRIBE",
		"params": []string{subject},
		"id":     len(b.activeStreams) + 1000,
	}
	_ = b.conn.WriteJSON(msg)

	delete(b.activeStreams, subject)
}

func (b *FuturesWebsocket) GetActiveStreams() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var streams []string
	for k := range b.activeStreams {
		streams = append(streams, k)
	}
	return streams
}

func (b *FuturesWebsocket) IsStreamActive(subject string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, ok := b.activeStreams[subject]
	return ok
}

func (b *FuturesWebsocket) Fetch(_ string) (domain.Message, error) {
	return domain.Message{}, fmt.Errorf("not supported: websocket provider does not implement fetch")
}

func (b *FuturesWebsocket) IsValidSubject(subject string, isFetch bool) bool {
	if isFetch {
		return false
	}
	return len(subject) > 0 // Extend with regex or lookup if needed
}

func (b *FuturesWebsocket) readLoop() {
	for {
		_, msgBytes, err := b.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				return
			}
			fmt.Printf("read error: %v\n", err)
			continue
		}

		var container struct {
			Stream string          `json:"stream"`
			Data   json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(msgBytes, &container); err != nil {
			continue
		}

		b.mu.Lock()
		ch, ok := b.activeStreams[container.Stream]
		b.mu.Unlock()
		if !ok {
			continue
		}

		msg := domain.Message{
			Identifier: domain.Identifier{
				Provider: "binance_futures_websocket",
				Subject:  container.Stream,
			},
			Payload: container.Data,
		}

		select {
		case ch <- msg:
		default:
			fmt.Printf("channel for %s is full, dropping message\n", container.Stream)
		}
	}
}
