// Package manager implements the core orchestration logic for data providers and client sessions
// in the tessera data_service. It manages provider registration, session lifecycle, client attachment,
// stream configuration, and routing of messages between clients and providers.
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
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/worker"
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
func NewManager(router *router.Router, workerRegistry *worker.Registry) *Manager {
	m := &Manager{
		cmdCh:     make(chan any, 256),
		providers: make(map[string]provider.Provider),
		sessions:  make(map[uuid.UUID]*session),
		router:    router,
	}
	go router.Start()
	go m.run()

	slog.Default().Info("manager started", slog.String("cmp", "manager"))

	return m
}

// API

// AddProvider adds and starts a new provider.
func (m *Manager) AddProvider(name string, p provider.Provider) error {
	slog.Default().Debug("add provider request", slog.String("cmp", "manager"), slog.String("name", name))
	resp := make(chan addProviderResult, 1)
	m.cmdCh <- addProviderCmd{name: name, p: p, resp: resp}

	r := <-resp

	slog.Default().Info("provider added", slog.String("cmp", "manager"), slog.String("name", name))
	return r.err
}

// RemoveProvider stops and removes a provider, cleaning up all sessions.
func (m *Manager) RemoveProvider(name string) error {
	slog.Default().Debug("remove provider request", slog.String("cmp", "manager"), slog.String("name", name))
	resp := make(chan removeProviderResult, 1)
	m.cmdCh <- removeProviderCmd{name: name, resp: resp}

	r := <-resp

	slog.Default().Info("provider removed", slog.String("cmp", "manager"), slog.String("name", name))
	return r.err
}

// NewSession creates a new session with the given idle timeout.
func (m *Manager) NewSession(idleAfter time.Duration) uuid.UUID {
	slog.Default().Debug("new session request", slog.String("cmp", "manager"), slog.Duration("idle_after", idleAfter))
	resp := make(chan newSessionResult, 1)
	m.cmdCh <- newSessionCmd{idleAfter: idleAfter, resp: resp}

	r := <-resp

	slog.Default().Info("new session created", slog.String("cmp", "manager"), slog.String("session", r.id.String()))
	return r.id
}

// AttachClient attaches a client to a session, creates and returns client channels for the session.
func (m *Manager) AttachClient(id uuid.UUID, inBuf, outBuf int) (chan<- domain.Message, <-chan domain.Message, error) {
	slog.Default().Debug("attach client request", slog.String("cmp", "manager"), slog.String("session", id.String()), slog.Int("in_buf", inBuf), slog.Int("out_buf", outBuf))
	resp := make(chan attachResult, 1)
	m.cmdCh <- attachCmd{sid: id, inBuf: inBuf, outBuf: outBuf, resp: resp}

	r := <-resp

	slog.Default().Info("client attached", slog.String("cmp", "manager"), slog.String("session", id.String()))
	return r.cin, r.cout, r.err
}

// DetachClient detaches the client from the session, closes client channels and arms timeout.
func (m *Manager) DetachClient(id uuid.UUID) error {
	slog.Default().Debug("detach client request", slog.String("cmp", "manager"), slog.String("session", id.String()))
	resp := make(chan detachResult, 1)
	m.cmdCh <- detachCmd{sid: id, resp: resp}

	r := <-resp

	slog.Default().Info("client detached", slog.String("cmp", "manager"), slog.String("session", id.String()))
	return r.err
}

// ConfigureSession sets the next set of patterns for the session, starting and stopping streams as needed.
func (m *Manager) ConfigureSession(id uuid.UUID, next []domain.Pattern) error {
	slog.Default().Debug("configure session request", slog.String("cmp", "manager"), slog.String("session", id.String()), slog.Int("patterns", len(next)))
	resp := make(chan configureResult, 1)
	m.cmdCh <- configureCmd{sid: id, next: next, resp: resp}

	r := <-resp

	slog.Default().Info("session configured", slog.String("cmp", "manager"), slog.String("session", id.String()), slog.String("err", fmt.Sprintf("%v", r.err)))
	return r.err
}

// CloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) CloseSession(id uuid.UUID) error {
	slog.Default().Debug("close session request", slog.String("cmp", "manager"), slog.String("session", id.String()))
	resp := make(chan closeSessionResult, 1)
	m.cmdCh <- closeSessionCmd{sid: id, resp: resp}

	r := <-resp

	slog.Default().Info("session closed", slog.String("cmp", "manager"), slog.String("session", id.String()))
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

// handleAddProvider adds and starts a new provider.
func (m *Manager) handleAddProvider(cmd addProviderCmd) {
	if _, ok := m.providers[cmd.name]; ok {
		slog.Default().Warn("provider already exists", slog.String("cmp", "manager"), slog.String("name", cmd.name))
		cmd.resp <- addProviderResult{err: fmt.Errorf("provider exists: %s", cmd.name)}
		return
	}
	if err := cmd.p.Start(); err != nil {
		slog.Default().Warn("failed to start provider", slog.String("cmp", "manager"), slog.String("name", cmd.name), slog.String("err", err.Error()))
		cmd.resp <- addProviderResult{err: fmt.Errorf("failed to start provider %s: %w", cmd.name, err)}
		return
	}
	m.providers[cmd.name] = cmd.p
	cmd.resp <- addProviderResult{err: nil}
}

// handleRemoveProvider stops and removes a provider, removing the bindings from all sessions that use streams from it.
// TODO: Implement this function.
func (m *Manager) handleRemoveProvider(_ removeProviderCmd) {
	panic("unimplemented")
}

// handleNewSession creates a new session with the given idle timeout. The idle timeout is typically not set by the client, but by the server configuration.
func (m *Manager) handleNewSession(cmd newSessionCmd) {
	s := newSession(cmd.idleAfter)

	// Only arm the idle timer if the timeout is positive. We allow a zero or negative timeout to indicate "never timeout".
	if s.idleAfter <= 0 {
		s.armIdleTimer(func() {
			resp := make(chan closeSessionResult, 1)
			m.cmdCh <- closeSessionCmd{sid: s.id, resp: resp}
			<-resp
		})
	}

	m.sessions[s.id] = s

	cmd.resp <- newSessionResult{id: s.id}
}

// handleAttach attaches a client to a session, creating new client channels for the session. If the session is already attached, returns an error.
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

// handleDetach detaches the client from the session, closing client channels and arming the idle timeout. If the session is not attached, returns an error.
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

	// Only rearm the idle timer if the timeout is positive.
	if s.idleAfter > 0 {
		s.armIdleTimer(func() {
			resp := make(chan closeSessionResult, 1)
			m.cmdCh <- closeSessionCmd{sid: s.id, resp: resp}
			<-resp
		})
	}

	s.attached = false

	cmd.resp <- detachResult{nil}
}

// handleConfigure updates the session bindings, starting and stopping streams as needed. Currently only supports Raw streams.
// TODO: Change this configuration to be an atomic operation, so that partial failures do not end in a half-configured state.
func (m *Manager) handleConfigure(cmd configureCmd) {
	_, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- configureResult{ErrSessionNotFound}
		return
	}

	var errs error

	cmd.resp <- configureResult{err: errs}
}

// handleCloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) handleCloseSession(cmd closeSessionCmd) {
	_, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- closeSessionResult{err: ErrSessionNotFound}
		return
	}

	var errs error

	cmd.resp <- closeSessionResult{err: errs}
}
