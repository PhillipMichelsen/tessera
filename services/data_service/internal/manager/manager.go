package manager

import (
	"fmt"
	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
	"sync"
	"time"
)

type Manager struct {
	providers       map[string]provider.Provider
	providerStreams map[domain.Identifier]chan domain.Message

	clientStreams map[uuid.UUID]*ClientStream

	router *router.Router

	mu sync.Mutex
}

type ClientStream struct {
	UUID        uuid.UUID
	Identifiers []domain.Identifier
	OutChannel  chan domain.Message
	Timer       *time.Timer
}

func NewManager(router *router.Router) *Manager {
	go router.Run() // Start the router in a separate goroutine
	return &Manager{
		providers:       make(map[string]provider.Provider),
		providerStreams: make(map[domain.Identifier]chan domain.Message),
		clientStreams:   make(map[uuid.UUID]*ClientStream),
		router:          router,
	}
}

func (m *Manager) StartStream(ids []domain.Identifier) (uuid.UUID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate Identifiers, prevents unnecessary calls to providers
	for _, id := range ids {
		if id.Provider == "" || id.Subject == "" {
			return uuid.Nil, fmt.Errorf("invalid identifier: %v", id)
		}
		if _, ok := m.providers[id.Provider]; !ok {
			return uuid.Nil, fmt.Errorf("unknown provider: %s", id.Provider)
		}
		if !m.providers[id.Provider].IsValidSubject(id.Subject, false) {
			return uuid.Nil, fmt.Errorf("invalid subject for provider %s: %s", id.Provider, id.Subject)
		}
	}

	// Provision provider streams
	for _, id := range ids {
		if _, ok := m.providerStreams[id]; ok {
			continue // Skip if requested stream is already being provided
		}

		ch := make(chan domain.Message, 64)
		if err := m.providers[id.Provider].RequestStream(id.Subject, ch); err != nil {
			return uuid.Nil, fmt.Errorf("provision %v: %w", id, err)
		}
		m.providerStreams[id] = ch

		// Start routine to route the provider stream to the router's input channel
		go func(ch chan domain.Message) {
			for msg := range ch {
				m.router.IncomingChannel() <- msg
			}
		}(ch)
	}

	streamID := uuid.New()

	m.clientStreams[streamID] = &ClientStream{
		UUID:        streamID,
		Identifiers: ids,
		OutChannel:  nil, // Initially nil, will be set when connected
		Timer: time.AfterFunc(1*time.Minute, func() {
			fmt.Printf("stream %s expired due to inactivity\n", streamID)
			m.StopStream(streamID)
		}),
	}

	return streamID, nil
}

func (m *Manager) StopStream(streamID uuid.UUID) {
	m.DisconnectStream(streamID)

	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok {
		return // Stream not found
	}

	stream.Timer.Stop()

	delete(m.clientStreams, streamID)

	// Find provider streams that are used by other client streams
	used := make(map[domain.Identifier]bool)
	for _, s := range m.clientStreams {
		for _, id := range s.Identifiers {
			used[id] = true
		}
	}

	// Cancel provider streams that are not used by any client stream
	for id, ch := range m.providerStreams {
		if !used[id] {
			m.providers[id.Provider].CancelStream(id.Subject)
			close(ch)
			delete(m.providerStreams, id)
		}
	}
}

func (m *Manager) ConnectStream(streamID uuid.UUID) (<-chan domain.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok {
		return nil, fmt.Errorf("stream not found: %s", streamID)
	}

	if stream.OutChannel != nil {
		return nil, fmt.Errorf("stream already connected")
	}

	ch := make(chan domain.Message, 128)
	stream.OutChannel = ch

	for _, ident := range stream.Identifiers {
		m.router.RegisterRoute(ident, ch)
	}

	if stream.Timer != nil {
		stream.Timer.Stop()
		stream.Timer = nil
	}

	return ch, nil
}

func (m *Manager) DisconnectStream(streamID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok || stream.OutChannel == nil {
		return // already disconnected or does not exist
	}

	// Deregister all identifiers from the router
	for _, ident := range stream.Identifiers {
		m.router.DeregisterRoute(ident, stream.OutChannel)
	}

	// Close the output channel
	close(stream.OutChannel)
	stream.OutChannel = nil

	// Set up the expiry timer
	stream.Timer = time.AfterFunc(1*time.Minute, func() {
		fmt.Printf("stream %s expired due to inactivity\n", streamID)
		m.StopStream(streamID)
	})
}

func (m *Manager) AddProvider(name string, p provider.Provider) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.providers[name]; exists {
		panic(fmt.Sprintf("provider %s already exists", name))
	}

	if err := p.Start(); err != nil {
		panic(fmt.Errorf("failed to start provider %s: %w", name, err))
	}

	m.providers[name] = p
}

func (m *Manager) RemoveProvider(name string) {
	panic("not implemented yet") // TODO: Implement provider removal logic
}
