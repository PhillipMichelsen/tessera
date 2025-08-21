package manager

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
)

var (
	ErrSessionNotFound   = errors.New("session not found")
	ErrSessionClosed     = errors.New("session closed")
	ErrOutboundFull      = errors.New("session publish buffer full")
	ErrInvalidIdentifier = errors.New("invalid identifier")
	ErrUnknownProvider   = errors.New("unknown provider")
)

type Manager struct {
	providers         map[string]provider.Provider
	providerStreams   map[domain.Identifier]chan domain.Message
	rawReferenceCount map[domain.Identifier]int

	sessions map[uuid.UUID]*session

	router *router.Router
	mu     sync.Mutex
}

type session struct {
	id         uuid.UUID
	in         chan domain.Message
	mailbox    chan domain.Message
	out        chan domain.Message
	bound      map[domain.Identifier]struct{}
	dropOnFull bool
	closed     bool
}

func NewManager(r *router.Router) *Manager {
	go r.Run()
	return &Manager{
		providers:         make(map[string]provider.Provider),
		providerStreams:   make(map[domain.Identifier]chan domain.Message),
		rawReferenceCount: make(map[domain.Identifier]int),
		sessions:          make(map[uuid.UUID]*session),
		router:            r,
	}
}

func (m *Manager) NewSession(bufIn, bufOut int, dropOnFull bool) (uuid.UUID, chan<- domain.Message, <-chan domain.Message, error) {
	if bufIn <= 0 {
		bufIn = 1024
	}
	if bufOut <= 0 {
		bufOut = 1024
	}

	s := &session{
		id:         uuid.New(),
		in:         make(chan domain.Message, bufIn),
		mailbox:    make(chan domain.Message, bufOut),
		out:        make(chan domain.Message, bufOut),
		bound:      make(map[domain.Identifier]struct{}),
		dropOnFull: dropOnFull,
	}

	m.mu.Lock()
	m.sessions[s.id] = s
	incoming := m.router.IncomingChannel()
	m.mu.Unlock()

	go func() {
		for msg := range s.in {
			incoming <- msg
		}
	}()

	go func() {
		for msg := range s.mailbox {
			s.out <- msg
		}
		close(s.out)
	}()

	return s.id, s.in, s.out, nil
}

func (m *Manager) CloseSession(id uuid.UUID) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	if s.closed {
		m.mu.Unlock()
		return nil
	}
	s.closed = true

	var ids []domain.Identifier
	for k := range s.bound {
		ids = append(ids, k)
		delete(s.bound, k)
	}
	delete(m.sessions, id)
	m.mu.Unlock()

	for _, ident := range ids {
		m.router.DeregisterRoute(ident, s.mailbox)
		if ident.IsRaw() {
			m.releaseRawStreamIfUnused(ident)
		}
	}

	close(s.mailbox)
	close(s.in)
	return nil
}

func (m *Manager) Subscribe(id uuid.UUID, ids ...domain.Identifier) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		return ErrSessionNotFound
	}

	for _, ident := range ids {
		m.mu.Lock()
		if _, exists := s.bound[ident]; exists {
			m.mu.Unlock()
			continue
		}
		m.mu.Unlock()

		if ident.IsRaw() {
			if err := m.provisionRawStream(ident); err != nil {
				return err
			}
		}

		m.mu.Lock()
		s.bound[ident] = struct{}{}
		m.mu.Unlock()
		m.router.RegisterRoute(ident, s.mailbox)
	}
	return nil
}

func (m *Manager) Unsubscribe(id uuid.UUID, ids ...domain.Identifier) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		return ErrSessionNotFound
	}

	for _, ident := range ids {
		m.mu.Lock()
		if _, exists := s.bound[ident]; !exists {
			m.mu.Unlock()
			continue
		}
		delete(s.bound, ident)
		m.mu.Unlock()

		m.router.DeregisterRoute(ident, s.mailbox)
		if ident.IsRaw() {
			m.releaseRawStreamIfUnused(ident)
		}
	}
	return nil
}

func (m *Manager) SetSubscriptions(id uuid.UUID, next []domain.Identifier) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	old := make(map[domain.Identifier]struct{}, len(s.bound))
	for k := range s.bound {
		old[k] = struct{}{}
	}
	m.mu.Unlock()

	toAdd, toDel := m.identifierSetDifferences(old, next)
	if len(toAdd) > 0 {
		if err := m.Subscribe(id, toAdd...); err != nil {
			return err
		}
	}
	if len(toDel) > 0 {
		if err := m.Unsubscribe(id, toDel...); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Publish(id uuid.UUID, msg domain.Message) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	if s.closed {
		m.mu.Unlock()
		return ErrSessionClosed
	}
	ch := s.in
	drop := s.dropOnFull
	m.mu.Unlock()

	if drop {
		select {
		case ch <- msg:
			return nil
		default:
			return ErrOutboundFull
		}
	}
	ch <- msg
	return nil
}

func (m *Manager) AddProvider(name string, p provider.Provider) error {
	m.mu.Lock()
	if _, exists := m.providers[name]; exists {
		m.mu.Unlock()
		return fmt.Errorf("provider exists: %s", name)
	}
	m.mu.Unlock()

	if err := p.Start(); err != nil {
		return fmt.Errorf("start provider %s: %w", name, err)
	}

	m.mu.Lock()
	m.providers[name] = p
	m.mu.Unlock()
	return nil
}

func (m *Manager) RemoveProvider(name string) error {
	m.mu.Lock()
	_, ok := m.providers[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("provider not found: %s", name)
	}
	return fmt.Errorf("RemoveProvider not implemented")
}

// helpers

func (m *Manager) provisionRawStream(id domain.Identifier) error {
	providerName, subject, ok := id.ProviderSubject()
	if !ok || providerName == "" || subject == "" {
		return ErrInvalidIdentifier
	}

	m.mu.Lock()
	prov, exists := m.providers[providerName]
	if !exists {
		m.mu.Unlock()
		return ErrUnknownProvider
	}
	if !prov.IsValidSubject(subject, false) {
		m.mu.Unlock()
		return fmt.Errorf("invalid subject %q for provider %s", subject, providerName)
	}

	if ch, ok := m.providerStreams[id]; ok {
		m.rawReferenceCount[id] = m.rawReferenceCount[id] + 1
		m.mu.Unlock()
		_ = ch
		return nil
	}

	ch := make(chan domain.Message, 64)
	if err := prov.RequestStream(subject, ch); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("provision %v: %w", id, err)
	}
	m.providerStreams[id] = ch
	m.rawReferenceCount[id] = 1
	incoming := m.router.IncomingChannel()
	m.mu.Unlock()

	go func(c chan domain.Message) {
		for msg := range c {
			incoming <- msg
		}
	}(ch)

	return nil
}

func (m *Manager) releaseRawStreamIfUnused(id domain.Identifier) {
	providerName, subject, ok := id.ProviderSubject()
	if !ok {
		return
	}

	m.mu.Lock()
	rc := m.rawReferenceCount[id] - 1
	if rc <= 0 {
		if ch, ok := m.providerStreams[id]; ok {
			if prov, exists := m.providers[providerName]; exists {
				prov.CancelStream(subject)
			}
			close(ch)
			delete(m.providerStreams, id)
		}
		delete(m.rawReferenceCount, id)
		m.mu.Unlock()
		return
	}
	m.rawReferenceCount[id] = rc
	m.mu.Unlock()
}

func (m *Manager) identifierSetDifferences(old map[domain.Identifier]struct{}, next []domain.Identifier) (toAdd, toDel []domain.Identifier) {
	newSet := make(map[domain.Identifier]struct{}, len(next))
	for _, id := range next {
		newSet[id] = struct{}{}
		if _, ok := old[id]; !ok {
			toAdd = append(toAdd, id)
		}
	}
	for id := range old {
		if _, ok := newSet[id]; !ok {
			toDel = append(toDel, id)
		}
	}
	return
}
