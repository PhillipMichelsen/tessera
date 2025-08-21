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
	go router.Run()
	return &Manager{
		providers:       make(map[string]provider.Provider),
		providerStreams: make(map[domain.Identifier]chan domain.Message),
		clientStreams:   make(map[uuid.UUID]*ClientStream),
		router:          router,
	}
}

func (m *Manager) StartClientStream() (uuid.UUID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	streamID := uuid.New()
	m.clientStreams[streamID] = &ClientStream{
		UUID:        streamID,
		Identifiers: nil,
		OutChannel:  nil,
		Timer: time.AfterFunc(1*time.Minute, func() {
			fmt.Printf("stream %s expired due to inactivity\n", streamID)
			err := m.StopClientStream(streamID)
			if err != nil {
				fmt.Printf("failed to stop stream after timeout: %v\n", err)
			}
		}),
	}

	return streamID, nil
}

func (m *Manager) ConfigureClientStream(streamID uuid.UUID, newIds []domain.Identifier) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok {
		return fmt.Errorf("stream not found: %s", streamID)
	}

	for _, id := range newIds {
		if id.IsRaw() {
			providerName, subject, ok := id.ProviderSubject()
			if !ok || providerName == "" || subject == "" {
				return fmt.Errorf("empty identifier: %v", id)
			}
			prov, exists := m.providers[providerName]
			if !exists {
				return fmt.Errorf("unknown provider: %s", providerName)
			}
			if !prov.IsValidSubject(subject, false) {
				return fmt.Errorf("invalid subject %q for provider %s", subject, providerName)
			}
		}
	}

	oldSet := make(map[domain.Identifier]struct{}, len(stream.Identifiers))
	for _, id := range stream.Identifiers {
		oldSet[id] = struct{}{}
	}
	newSet := make(map[domain.Identifier]struct{}, len(newIds))
	for _, id := range newIds {
		newSet[id] = struct{}{}
	}

	for _, id := range newIds {
		if _, seen := oldSet[id]; !seen {
			if id.IsRaw() {
				if _, ok := m.providerStreams[id]; !ok {
					ch := make(chan domain.Message, 64)
					providerName, subject, _ := id.ProviderSubject()
					if err := m.providers[providerName].RequestStream(subject, ch); err != nil {
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
			}

			if stream.OutChannel != nil {
				m.router.RegisterRoute(id, stream.OutChannel)
			}
		}
	}

	for _, oldId := range stream.Identifiers {
		if _, keep := newSet[oldId]; !keep {
			if stream.OutChannel != nil {
				m.router.DeregisterRoute(oldId, stream.OutChannel)
			}
		}
	}

	stream.Identifiers = newIds

	used := make(map[domain.Identifier]bool)
	for _, cs := range m.clientStreams {
		for _, id := range cs.Identifiers {
			if id.IsRaw() {
				used[id] = true
			}
		}
	}
	for id, ch := range m.providerStreams {
		if !used[id] {
			providerName, subject, _ := id.ProviderSubject()
			m.providers[providerName].CancelStream(subject)
			close(ch)
			delete(m.providerStreams, id)
		}
	}

	return nil
}

func (m *Manager) StopClientStream(streamID uuid.UUID) error {
	m.DisconnectClientStream(streamID)

	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok {
		return fmt.Errorf("stream not found: %s", streamID)
	}

	stream.Timer.Stop()

	delete(m.clientStreams, streamID)

	used := make(map[domain.Identifier]bool)
	for _, s := range m.clientStreams {
		for _, id := range s.Identifiers {
			if id.IsRaw() {
				used[id] = true
			}
		}
	}

	for id, ch := range m.providerStreams {
		if !used[id] {
			providerName, subject, _ := id.ProviderSubject()
			m.providers[providerName].CancelStream(subject)
			close(ch)
			delete(m.providerStreams, id)
		}
	}

	return nil
}

func (m *Manager) ConnectClientStream(streamID uuid.UUID) (<-chan domain.Message, error) {
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

func (m *Manager) DisconnectClientStream(streamID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.clientStreams[streamID]
	if !ok || stream.OutChannel == nil {
		return
	}

	for _, ident := range stream.Identifiers {
		m.router.DeregisterRoute(ident, stream.OutChannel)
	}

	close(stream.OutChannel)
	stream.OutChannel = nil

	stream.Timer = time.AfterFunc(1*time.Minute, func() {
		fmt.Printf("stream %s expired due to inactivity\n", streamID)
		err := m.StopClientStream(streamID)
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

func (m *Manager) RemoveProvider(_ string) {
	panic("not implemented yet")
}
