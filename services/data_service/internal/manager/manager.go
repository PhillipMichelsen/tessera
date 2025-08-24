package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
)

var (
	ErrSessionNotFound    = errors.New("session not found")
	ErrSessionClosed      = errors.New("session closed")
	ErrInvalidIdentifier  = errors.New("invalid identifier")
	ErrUnknownProvider    = errors.New("unknown provider")
	ErrClientAlreadyBound = errors.New("client channels already bound")
)

const (
	defaultInternalBuf = 1024
	defaultClientBuf   = 256
)

type ChannelOpts struct {
	InBufSize  int
	OutBufSize int
	// If true, drop to clientOut when its buffer is full. If false, block.
	DropOutbound bool
}

// Manager owns providers, sessions, and the router fanout.
type Manager struct {
	providers         map[string]provider.Provider
	providerStreams   map[domain.Identifier]chan domain.Message
	rawReferenceCount map[domain.Identifier]int

	sessions map[uuid.UUID]*session

	router *router.Router
	mu     sync.Mutex
}

type session struct {
	id uuid.UUID

	// Stable internal channels. Only the session writes internalOut and reads internalIn.
	internalIn  chan domain.Message // forwarded into router.IncomingChannel()
	internalOut chan domain.Message // registered as router route target

	// Current client attachment (optional). Created by GetChannels.
	clientIn  chan domain.Message // caller writes
	clientOut chan domain.Message // caller reads

	// Cancels the permanent internalIn forwarder.
	cancelInternal context.CancelFunc
	// Cancels current client forwarders.
	cancelClient context.CancelFunc

	bound     map[domain.Identifier]struct{}
	closed    bool
	idleAfter time.Duration
	idleTimer *time.Timer
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

// NewSession creates a session with stable internal channels and a permanent
// forwarder that pipes internalIn into router.IncomingChannel().
func (m *Manager) NewSession(idleAfter time.Duration) (uuid.UUID, error) {
	s := &session{
		id:          uuid.New(),
		internalIn:  make(chan domain.Message, defaultInternalBuf),
		internalOut: make(chan domain.Message, defaultInternalBuf),
		bound:       make(map[domain.Identifier]struct{}),
		idleAfter:   idleAfter,
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelInternal = cancel

	m.mu.Lock()
	m.sessions[s.id] = s
	incoming := m.router.IncomingChannel()
	m.mu.Unlock()

	// Permanent forwarder: internalIn -> router.Incoming
	go func(ctx context.Context, in <-chan domain.Message) {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-in:
				if !ok {
					return
				}
				// Place to filter, validate, meter, or throttle.
				incoming <- msg
			}
		}
	}(ctx, s.internalIn)

	return s.id, nil
}

// GetChannels creates a fresh client attachment and hooks both directions:
// clientIn -> internalIn and internalOut -> clientOut. Only one attachment at a time.
func (m *Manager) GetChannels(id uuid.UUID, opts ChannelOpts) (chan<- domain.Message, <-chan domain.Message, error) {
	if opts.InBufSize <= 0 {
		opts.InBufSize = defaultClientBuf
	}
	if opts.OutBufSize <= 0 {
		opts.OutBufSize = defaultClientBuf
	}

	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return nil, nil, ErrSessionNotFound
	}
	if s.closed {
		m.mu.Unlock()
		return nil, nil, ErrSessionClosed
	}
	if s.clientIn != nil || s.clientOut != nil {
		m.mu.Unlock()
		return nil, nil, ErrClientAlreadyBound
	}

	// Create attachment channels.
	cin := make(chan domain.Message, opts.InBufSize)
	cout := make(chan domain.Message, opts.OutBufSize)
	s.clientIn, s.clientOut = cin, cout

	// Stop idle timer while attached.
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}

	internalIn := s.internalIn
	internalOut := s.internalOut

	// Prepare per-attachment cancel.
	attachCtx, attachCancel := context.WithCancel(context.Background())
	s.cancelClient = attachCancel

	m.mu.Unlock()

	// Forward clientIn -> internalIn
	go func(ctx context.Context, src <-chan domain.Message, dst chan<- domain.Message) {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-src:
				if !ok {
					// Client closed input; stop forwarding.
					return
				}
				// Per-client checks could go here.
				dst <- msg
			}
		}
	}(attachCtx, cin, internalIn)

	// Forward internalOut -> clientOut
	go func(ctx context.Context, src <-chan domain.Message, dst chan<- domain.Message, drop bool) {
		defer close(dst)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-src:
				if !ok {
					// Session is closing; signal EOF to client.
					return
				}
				if drop {
					select {
					case dst <- msg:
					default:
						// Drop on client backpressure. Add metrics if desired.
					}
				} else {
					dst <- msg
				}
			}
		}
	}(attachCtx, internalOut, cout, opts.DropOutbound)

	// Return directional views.
	return (chan<- domain.Message)(cin), (<-chan domain.Message)(cout), nil
}

// DetachClient cancels current client forwarders and clears the attachment.
// It starts the idle close timer if configured.
func (m *Manager) DetachClient(id uuid.UUID) error {
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
	// Capture and clear client state.
	cancel := s.cancelClient
	cin := s.clientIn
	s.cancelClient = nil
	s.clientIn, s.clientOut = nil, nil
	after := s.idleAfter
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	// Close clientIn to terminate clientIn->internalIn forwarder if client forgot.
	if cin != nil {
		close(cin)
	}

	if after > 0 {
		m.mu.Lock()
		ss, ok := m.sessions[id]
		if ok && !ss.closed && ss.clientOut == nil && ss.idleTimer == nil {
			ss.idleTimer = time.AfterFunc(after, func() { _ = m.CloseSession(id) })
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) Subscribe(id uuid.UUID, ids ...domain.Identifier) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	out := s.internalOut
	m.mu.Unlock()

	for _, ident := range ids {
		m.mu.Lock()
		if _, exists := s.bound[ident]; exists {
			m.mu.Unlock()
			continue
		}
		s.bound[ident] = struct{}{}
		m.mu.Unlock()

		if ident.IsRaw() {
			if err := m.provisionRawStream(ident); err != nil {
				return err
			}
		}
		m.router.RegisterRoute(ident, out)
	}
	return nil
}

func (m *Manager) Unsubscribe(id uuid.UUID, ids ...domain.Identifier) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	out := s.internalOut
	m.mu.Unlock()

	for _, ident := range ids {
		m.mu.Lock()
		if _, exists := s.bound[ident]; !exists {
			m.mu.Unlock()
			continue
		}
		delete(s.bound, ident)
		m.mu.Unlock()

		m.router.DeregisterRoute(ident, out)
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
	out := s.internalOut
	m.mu.Unlock()

	toAdd, toDel := m.identifierSetDifferences(old, next)

	for _, ident := range toAdd {
		m.mu.Lock()
		s.bound[ident] = struct{}{}
		m.mu.Unlock()

		if ident.IsRaw() {
			if err := m.provisionRawStream(ident); err != nil {
				return err
			}
		}
		m.router.RegisterRoute(ident, out)
	}

	for _, ident := range toDel {
		m.mu.Lock()
		_, exists := s.bound[ident]
		delete(s.bound, ident)
		m.mu.Unlock()

		if exists {
			m.router.DeregisterRoute(ident, out)
			if ident.IsRaw() {
				m.releaseRawStreamIfUnused(ident)
			}
		}
	}
	return nil
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
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
	out := s.internalOut
	ids := make([]domain.Identifier, 0, len(s.bound))
	for k := range s.bound {
		ids = append(ids, k)
	}
	cancelInternal := s.cancelInternal
	cancelClient := s.cancelClient
	// Clear attachments before unlock to avoid races.
	s.cancelClient = nil
	cin := s.clientIn
	s.clientIn, s.clientOut = nil, nil
	delete(m.sessions, id)
	m.mu.Unlock()

	// Deregister all routes and release raw streams.
	for _, ident := range ids {
		m.router.DeregisterRoute(ident, out)
		if ident.IsRaw() {
			m.releaseRawStreamIfUnused(ident)
		}
	}

	// Stop forwarders and close internal channels.
	if cancelClient != nil {
		cancelClient()
	}
	if cancelInternal != nil {
		cancelInternal()
	}
	close(s.internalIn)
	close(s.internalOut) // will close clientOut via forwarder

	// Close clientIn to ensure its forwarder exits even if client forgot.
	if cin != nil {
		close(cin)
	}
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
	// Optional: implement full drain and cancel of all streams for this provider.
	return fmt.Errorf("RemoveProvider not implemented")
}

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

	// Provider stream -> router.Incoming
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
