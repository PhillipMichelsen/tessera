package manager

import (
	"context"
	"fmt"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/marketdata/internal/identifier"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/marketdata/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/marketdata/internal/router"
)

type Manager struct {
	Router   *router.Router
	incoming chan router.Message // Aggregated incoming messages from providers, for the Router

	Providers       map[string]provider.Provider
	providerStreams map[identifier.Identifier]chan router.Message // Channels for the streams that the providers are running

	subscribers map[identifier.Identifier][]chan router.Message // Map of identifiers to subscriber channels, one to many mapping

	mu sync.Mutex
}

// NewManager constructs a Manager and starts its Router loop.
func NewManager() *Manager {
	incoming := make(chan router.Message, 128)
	r := router.NewRouter(incoming)

	m := &Manager{
		Router:          r,
		incoming:        incoming,
		Providers:       make(map[string]provider.Provider),
		providerStreams: make(map[identifier.Identifier]chan router.Message),
		subscribers:     make(map[identifier.Identifier][]chan router.Message),
	}
	go r.Run()
	return m
}

// AddProvider registers and starts a new Provider under the given name.
func (m *Manager) AddProvider(name string, p provider.Provider) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.Providers[name]; exists {
		return fmt.Errorf("provider %q already exists", name)
	}
	if err := p.Start(); err != nil {
		return fmt.Errorf("failed to start provider %q: %w", name, err)
	}
	m.Providers[name] = p
	return nil
}

// RemoveProvider stops and unregisters a Provider, tearing down all its streams.
func (m *Manager) RemoveProvider(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, exists := m.Providers[name]
	if !exists {
		return fmt.Errorf("provider %q not found", name)
	}
	if err := p.Stop(); err != nil {
		return fmt.Errorf("failed to stop provider %q: %w", name, err)
	}

	// tear down every active stream for this provider
	for id, streamCh := range m.providerStreams {
		if id.Provider != name {
			continue
		}
		// stop the provider's internal stream
		p.CancelStream(id.Subject)
		close(streamCh)
		delete(m.providerStreams, id)

		// deregister & close all subscriber channels
		for _, subCh := range m.subscribers[id] {
			m.Router.Deregister(id, subCh)
			close(subCh)
		}
		delete(m.subscribers, id)
	}

	delete(m.Providers, name)
	return nil
}

// Stream establishes a single gRPC‐stream channel that multiplexes
// live updates for all requested identifiers.
// When ctx is canceled, it will deregister and—if a stream has no more
// subscribers—call CancelStream on the provider.
func (m *Manager) Stream(
	ctx context.Context,
	reqs []identifier.Identifier,
) (<-chan router.Message, error) {
	m.mu.Lock()
	// 1) Validate and ensure each provider/subject is streaming
	for _, id := range reqs {
		p, ok := m.Providers[id.Provider]
		if !ok {
			m.mu.Unlock()
			return nil, fmt.Errorf("provider %q not found", id.Provider)
		}
		if !p.IsValidSubject(id.Subject, false) {
			m.mu.Unlock()
			return nil, fmt.Errorf("invalid subject %q for provider %q", id.Subject, id.Provider)
		}
		// start the provider stream if not already running
		if _, exists := m.providerStreams[id]; !exists {
			ch := make(chan router.Message, 64)
			if err := p.RequestStream(id.Subject, ch); err != nil {
				m.mu.Unlock()
				return nil, fmt.Errorf("could not request stream for %v: %w", id, err)
			}
			m.providerStreams[id] = ch
			// pump into the central incoming channel
			go func(c chan router.Message) {
				for msg := range c {
					m.incoming <- msg
				}
			}(ch)
		}
	}

	// 2) Create one channel for this RPC and register it for every ID
	subCh := make(chan router.Message, 128)
	for _, id := range reqs {
		m.subscribers[id] = append(m.subscribers[id], subCh)
		m.Router.Register(id, subCh)
	}
	m.mu.Unlock()

	// 3) Teardown logic when context is done
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		defer m.mu.Unlock()

		for _, id := range reqs {
			// deregister this subscriber channel
			m.Router.Deregister(id, subCh)

			// remove it from the list
			subs := m.subscribers[id]
			for i, ch := range subs {
				if ch == subCh {
					subs = append(subs[:i], subs[i+1:]...)
					break
				}
			}

			if len(subs) == 0 {
				// no more listeners: cancel provider stream
				delete(m.subscribers, id)
				if streamCh, ok := m.providerStreams[id]; ok {
					m.Providers[id.Provider].CancelStream(id.Subject)
					close(streamCh)
					delete(m.providerStreams, id)
				}
			} else {
				m.subscribers[id] = subs
			}
		}

		close(subCh)
	}()

	return subCh, nil
}

// Fetch performs a single request/response fetch against the named provider.
func (m *Manager) Fetch(providerName, subject string) (router.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.Providers[providerName]
	if !ok {
		return router.Message{}, fmt.Errorf("provider %q not found", providerName)
	}
	if !p.IsValidSubject(subject, true) {
		return router.Message{}, fmt.Errorf("invalid subject %q for provider %q", subject, providerName)
	}
	return p.Fetch(subject)
}
