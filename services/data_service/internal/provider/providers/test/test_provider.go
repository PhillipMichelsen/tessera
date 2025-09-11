package test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type TestProvider struct {
	mu            sync.Mutex
	streams       map[string]*stream
	outputChannel chan<- domain.Message
	tickDuration  time.Duration
}

type stream struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewTestProvider wires the outbound channel.
func NewTestProvider(out chan<- domain.Message, tickDuration time.Duration) *TestProvider {
	return &TestProvider{
		streams:       make(map[string]*stream),
		outputChannel: out,
		tickDuration:  tickDuration,
	}
}

func (t *TestProvider) Start() error { return nil }

func (t *TestProvider) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for key, s := range t.streams {
		s.cancel()
		<-s.done
		delete(t.streams, key)
	}
}

func (t *TestProvider) Subscribe(subject string) <-chan error {
	errCh := make(chan error, 1)

	if !t.IsValidSubject(subject, false) {
		errCh <- errors.New("invalid subject")
		close(errCh)
		return errCh
	}

	t.mu.Lock()
	// Already active: treat as success.
	if _, ok := t.streams[subject]; ok {
		t.mu.Unlock()
		errCh <- nil
		return errCh
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{cancel: cancel, done: make(chan struct{})}
	t.streams[subject] = s
	out := t.outputChannel
	t.mu.Unlock()

	// Stream goroutine.
	go func(subj string, s *stream) {
		slog.Default().Debug("new stream routine started", slog.String("cmp", "test_provider"), slog.String("subject", subj))
		ticker := time.NewTicker(t.tickDuration)
		ident, _ := domain.RawID("test_provider", subj)
		defer func() {
			ticker.Stop()
			close(s.done)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if out != nil {
					msg := domain.Message{
						Identifier: ident,
						Payload:    []byte(time.Now().UTC().Format(time.RFC3339Nano)),
					}
					// Non-blocking send avoids deadlock if caller stops reading.
					select {
					case out <- msg:
					default:
						slog.Default().Warn("dropping message due to backpressure", "cmp", "test_provider", "subject", subj)
					}
				}
			}
		}
	}(subject, s)

	// Signal successful subscription.
	errCh <- nil
	return errCh
}

func (t *TestProvider) Unsubscribe(subject string) <-chan error {
	errCh := make(chan error, 1)

	t.mu.Lock()
	s, ok := t.streams[subject]
	if !ok {
		t.mu.Unlock()
		errCh <- errors.New("not subscribed")
		return errCh
	}
	delete(t.streams, subject)
	t.mu.Unlock()

	go func() {
		s.cancel()
		<-s.done
		errCh <- nil
	}()
	return errCh
}

func (t *TestProvider) Fetch(subject string) (domain.Message, error) {
	return domain.Message{}, fmt.Errorf("fetch not supported by provider")
}

func (t *TestProvider) GetActiveStreams() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]string, 0, len(t.streams))
	for k := range t.streams {
		keys = append(keys, k)
	}
	return keys
}

func (t *TestProvider) IsStreamActive(key string) bool {
	t.mu.Lock()
	_, ok := t.streams[key]
	t.mu.Unlock()
	return ok
}

func (t *TestProvider) IsValidSubject(key string, _ bool) bool {
	return key != ""
}
