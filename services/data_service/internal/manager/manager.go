package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
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

func (m *Manager) StartStream() (uuid.UUID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	streamID := uuid.New()
	m.clientStreams[streamID] = &ClientStream{
		UUID:        streamID,
		Identifiers: nil, // start empty
		OutChannel:  nil, // not yet connected
		Timer: time.AfterFunc(1*time.Minute, func() {
			fmt.Printf("stream %s expired due to inactivity\n", streamID)
			err := m.StopStream(streamID)
			if err != nil {
				fmt.Printf("failed to stop stream after timeout: %v\n", err)
			}
		}),
	}

	return streamID, nil
}

func (m *Manager) ConfigureStream(streamID uuid.UUID, newIds []domain.Identifier) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok {
		return fmt.Errorf("stream not found: %s", streamID)
	}

	// Validate new identifiers.
	for _, id := range newIds {
		if id.Provider == "" || id.Subject == "" {
			return fmt.Errorf("empty identifier: %v", id)
		}
		prov, exists := m.providers[id.Provider]
		if !exists {
			return fmt.Errorf("unknown provider: %s", id.Provider)
		}
		if !prov.IsValidSubject(id.Subject, false) {
			return fmt.Errorf("invalid subject %q for provider %s", id.Subject, id.Provider)
		}
	}

	// Generate old and new sets of identifiers
	oldSet := make(map[domain.Identifier]struct{}, len(stream.Identifiers))
	for _, id := range stream.Identifiers {
		oldSet[id] = struct{}{}
	}
	newSet := make(map[domain.Identifier]struct{}, len(newIds))
	for _, id := range newIds {
		newSet[id] = struct{}{}
	}

	// Add identifiers that are in newIds but not in oldSet
	for _, id := range newIds {
		if _, seen := oldSet[id]; !seen {
			// Provision the stream from the provider if needed
			if _, ok := m.providerStreams[id]; !ok {
				ch := make(chan domain.Message, 64)
				if err := m.providers[id.Provider].RequestStream(id.Subject, ch); err != nil {
					return fmt.Errorf("provision %v: %w", id, err)
				}
				m.providerStreams[id] = ch

				incomingChannel := m.router.IncomingChannel()
				go func(c chan domain.Message) {
					for msg := range c {
						incomingChannel <- msg
					}
				}(ch)
			}

			// Register the new identifier with the router, only if there's an active output channel (meaning the stream is connected)
			if stream.OutChannel != nil {
				m.router.RegisterRoute(id, stream.OutChannel)
			}
		}
	}

	// Remove identifiers that are in oldSet but not in newSet
	for _, oldId := range stream.Identifiers {
		if _, keep := newSet[oldId]; !keep {
			// Deregister the identifier from the router, only if there's an active output channel (meaning the stream is connected)
			if stream.OutChannel != nil {
				m.router.DeregisterRoute(oldId, stream.OutChannel)
			}
		}
	}

	// Set the new identifiers for the stream
	stream.Identifiers = newIds

	// Clean up provider streams that are no longer used
	used := make(map[domain.Identifier]bool)
	for _, cs := range m.clientStreams {
		for _, id := range cs.Identifiers {
			used[id] = true
		}
	}
	for id, ch := range m.providerStreams {
		if !used[id] {
			m.providers[id.Provider].CancelStream(id.Subject)
			close(ch)
			delete(m.providerStreams, id)
		}
	}

	return nil
}

func (m *Manager) StopStream(streamID uuid.UUID) error {
	m.DisconnectStream(streamID)

	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok {
		return fmt.Errorf("stream not found: %s", streamID)
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

	return nil
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
		err := m.StopStream(streamID)
		if err != nil {
			fmt.Printf("failed to stop stream after disconnect: %v\n", err)
		}
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
