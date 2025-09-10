package manager

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
)

var (
	ErrSessionNotFound       = errors.New("session not found")
	ErrClientAlreadyAttached = errors.New("client already attached")
	ErrClientNotAttached     = errors.New("client not attached")
)

// Manager is a single-goroutine actor that owns all state.
type Manager struct {
	// Command channel
	cmdCh chan any

	// State (loop-owned)
	providers map[string]provider.Provider
	sessions  map[uuid.UUID]*session

	// Router
	router *router.Router
}

// NewManager creates a manager and starts its run loop.
func NewManager(r *router.Router) *Manager {
	m := &Manager{
		cmdCh:     make(chan any, 256),
		providers: make(map[string]provider.Provider),
		sessions:  make(map[uuid.UUID]*session),
		router:    r,
	}
	go r.Run()
	go m.run()

	lg().Info("manager started")

	return m
}

// Public API (posts commands to loop)

// AddProvider adds and starts a new provider.
func (m *Manager) AddProvider(name string, p provider.Provider) error {
	lg().Debug("add provider request", slog.String("name", name))
	resp := make(chan addProviderResult, 1)
	m.cmdCh <- addProviderCmd{name: name, p: p, resp: resp}

	r := <-resp
	return r.err
}

// RemoveProvider stops and removes a provider, cleaning up all sessions.
func (m *Manager) RemoveProvider(name string) error {
	lg().Debug("remove provider request", slog.String("name", name))
	resp := make(chan removeProviderResult, 1)
	m.cmdCh <- removeProviderCmd{name: name, resp: resp}

	r := <-resp
	return r.err
}

// NewSession creates a new session with the given idle timeout.
func (m *Manager) NewSession(idleAfter time.Duration) uuid.UUID {
	lg().Debug("new session request", slog.Duration("idle_after", idleAfter))
	resp := make(chan newSessionResult, 1)
	m.cmdCh <- newSessionCmd{idleAfter: idleAfter, resp: resp}

	r := <-resp
	return r.id
}

// AttachClient attaches a client to a session, creates and returns client channels for the session.
func (m *Manager) AttachClient(id uuid.UUID, inBuf, outBuf int) (chan<- domain.Message, <-chan domain.Message, error) {
	lg().Debug("attach client request", slog.String("session", id.String()), slog.Int("in_buf", inBuf), slog.Int("out_buf", outBuf))
	resp := make(chan attachResult, 1)
	m.cmdCh <- attachCmd{sid: id, inBuf: inBuf, outBuf: outBuf, resp: resp}

	r := <-resp
	return r.cin, r.cout, r.err
}

// DetachClient detaches the client from the session, closes client channels and arms timeout.
func (m *Manager) DetachClient(id uuid.UUID) error {
	lg().Debug("detach client request", slog.String("session", id.String()))
	resp := make(chan detachResult, 1)
	m.cmdCh <- detachCmd{sid: id, resp: resp}

	r := <-resp
	return r.err
}

// ConfigureSession sets the next set of identifiers for the session, starting and stopping streams as needed.
func (m *Manager) ConfigureSession(id uuid.UUID, next []domain.Identifier) error {
	lg().Debug("configure session request", slog.String("session", id.String()), slog.Int("idents", len(next)))
	resp := make(chan configureResult, 1)
	m.cmdCh <- configureCmd{sid: id, next: next, resp: resp}

	r := <-resp
	return r.err
}

// CloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) CloseSession(id uuid.UUID) error {
	lg().Debug("close session request", slog.String("session", id.String()))
	resp := make(chan closeSessionResult, 1)
	m.cmdCh <- closeSessionCmd{sid: id, resp: resp}

	r := <-resp
	return r.err
}

// The main loop of the manager, processing commands serially.
func (m *Manager) run() {
	for {
		msg := <-m.cmdCh
		switch c := msg.(type) {
		case addProviderCmd:
			m.handleAddProvider(c)
		case removeProviderCmd:
			m.handleRemoveProvider(c)
		case newSessionCmd:
			m.handleNewSession(c)
		case attachCmd:
			m.handleAttach(c)
		case detachCmd:
			m.handleDetach(c)
		case configureCmd:
			m.handleConfigure(c)
		case closeSessionCmd:
			m.handleCloseSession(c)
		}
	}
}

// Command handlers, run in loop goroutine. With a single goroutine, no locking is needed.

func (m *Manager) handleAddProvider(cmd addProviderCmd) {
	if _, ok := m.providers[cmd.name]; ok {
		lg().Warn("provider already exists", slog.String("name", cmd.name))
		cmd.resp <- addProviderResult{err: fmt.Errorf("provider exists: %s", cmd.name)}
		return
	}
	if err := cmd.p.Start(); err != nil {
		lg().Warn("failed to start provider", slog.String("name", cmd.name), slog.String("err", err.Error()))
		cmd.resp <- addProviderResult{err: fmt.Errorf("failed to start provider %s: %w", cmd.name, err)}
		return
	}
	m.providers[cmd.name] = cmd.p
	cmd.resp <- addProviderResult{err: nil}
}

func (m *Manager) handleRemoveProvider(cmd removeProviderCmd) {
	panic("unimplemented")
}

func (m *Manager) handleNewSession(cmd newSessionCmd) {
	s := newSession(cmd.idleAfter)
	s.armIdleTimer(func() {
		resp := make(chan closeSessionResult, 1)
		m.cmdCh <- closeSessionCmd{sid: s.id, resp: resp}
		<-resp
	})

	m.sessions[s.id] = s

	cmd.resp <- newSessionResult{id: s.id}
}

func (m *Manager) handleAttach(cmd attachCmd) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- attachResult{nil, nil, ErrSessionNotFound}
		return
	}
	if s.attached {
		cmd.resp <- attachResult{nil, nil, ErrClientAlreadyAttached}
		return
	}

	cin, cout := s.generateNewChannels(cmd.inBuf, cmd.outBuf)
	s.attached = true
	s.disarmIdleTimer()

	cmd.resp <- attachResult{cin: cin, cout: cout, err: nil}
}

func (m *Manager) handleDetach(cmd detachCmd) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- detachResult{ErrSessionNotFound}
		return
	}
	if !s.attached {
		cmd.resp <- detachResult{ErrClientNotAttached}
		return
	}

	s.clearChannels()
	s.armIdleTimer(func() {
		resp := make(chan closeSessionResult, 1)
		m.cmdCh <- closeSessionCmd{sid: s.id, resp: resp}
		<-resp
	})

	s.attached = false

	cmd.resp <- detachResult{nil}
}

// handleConfigure updates the session bindings, starting and stopping streams as needed. Currently only supports Raw streams.
func (m *Manager) handleConfigure(cmd configureCmd) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- configureResult{ErrSessionNotFound}
		return
	}

	toAdd, toRemove := identifierSetDifferences(identifierMapToSlice(s.bound), cmd.next)

	pendingSub := make(map[domain.Identifier]<-chan error)
	pendingUnsub := make(map[domain.Identifier]<-chan error)
	var added, removed []domain.Identifier
	var errs error

	// Adds
	for _, id := range toAdd {
		pName, subject, ok := id.ProviderSubject()
		if !ok || subject == "" || pName == "" {
			errs = errors.Join(errs, fmt.Errorf("invalid identifier: %s", id.Key()))
			continue
		}
		p, ok := m.providers[pName]
		if !ok {
			errs = errors.Join(errs, fmt.Errorf("provider not found: %s", pName))
			continue
		}
		if p.IsStreamActive(subject) {
			s.bound[id] = struct{}{}
			added = append(added, id)
			continue
		}
		pendingSub[id] = p.Subscribe(subject)
	}

	// Removes
	for _, id := range toRemove {
		pName, subject, ok := id.ProviderSubject()
		if !ok || subject == "" || pName == "" {
			errs = errors.Join(errs, fmt.Errorf("invalid identifier: %s", id.Key()))
			continue
		}
		p, ok := m.providers[pName]
		if !ok {
			errs = errors.Join(errs, fmt.Errorf("provider not found: %s", pName))
			continue
		}
		stillNeeded := false
		for _, other := range m.sessions {
			if other.id == s.id {
				continue
			}
			if _, bound := other.bound[id]; bound {
				stillNeeded = true
				break
			}
		}
		if stillNeeded {
			delete(s.bound, id)
			removed = append(removed, id)
			continue
		}
		pendingUnsub[id] = p.Unsubscribe(subject)
	}

	// Wait for subscribes
	for id, ch := range pendingSub {
		if err := <-ch; err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to subscribe to %s: %w", id.Key(), err))
			continue
		}
		s.bound[id] = struct{}{}
		added = append(added, id)
	}

	// Wait for unsubscribes
	for id, ch := range pendingUnsub {
		if err := <-ch; err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to unsubscribe from %s: %w", id.Key(), err))
			continue
		}
		delete(s.bound, id)
		removed = append(removed, id)
	}

	// Update the router routes to reflect the new successful bindings
	for _, id := range added {
		m.router.RegisterRoute(id, s.outChannel)
	}
	for _, id := range removed {
		m.router.DeregisterRoute(id, s.outChannel)
	}

	cmd.resp <- configureResult{err: errs}
}

func (m *Manager) handleCloseSession(c closeSessionCmd) {
	panic("unimplemented")
}
