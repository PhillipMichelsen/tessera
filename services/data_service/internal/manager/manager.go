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
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/worker"
)

var (
	ErrSessionNotFound       = errors.New("session not found")
	ErrSessionAlreadyAquired = errors.New("session already aquried")
)

// Manager is a single-goroutine actor that owns all state.
type Manager struct {
	// Command channel
	cmdCh chan any

	// State (loop-owned)
	sessions map[uuid.UUID]*session

	// Router
	router *router.Router
}

// NewManager creates a manager and starts its run loop.
func NewManager(router *router.Router, workerRegistry *worker.Registry) *Manager {
	m := &Manager{
		cmdCh:    make(chan any, 256),
		sessions: make(map[uuid.UUID]*session),
		router:   router,
	}
	go router.Start()
	go m.run()

	slog.Default().Info("manager started", slog.String("cmp", "manager"))

	return m
}

// API

// CreateSession creates a new session with the given idle timeout.
func (m *Manager) CreateSession(idleAfter time.Duration) uuid.UUID {
	slog.Default().Debug("create session request", slog.String("cmp", "manager"), slog.Duration("idle_after", idleAfter))
	resp := make(chan createSessionResult, 1)
	m.cmdCh <- createSessionCommand{idleAfter: idleAfter, resp: resp}

	r := <-resp

	slog.Default().Info("new session created", slog.String("cmp", "manager"), slog.String("session", r.sid.String()))
	return r.sid
}

// LeaseReceiver leases a receiver for the session and returns the receive func along with a close func.
// A session only permits one receiver to be lease at a time (may be subject to change)
func (m *Manager) LeaseReceiver(sid uuid.UUID) (func() (domain.Message, error), func(), error) {
	slog.Default().Debug("lease receiver request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan leaseReceiverResult, 1)
	m.cmdCh <- leaseReceiverCommand{sid: sid}

	r := <-resp

	slog.Default().Info("receiver leased", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	return r.receiveFunc, r.closeFunc, r.err
}

// LeaseSender leases a sender for the session and returns the send func along with a close func.
// A session only permits one sender to be lease at a time (may be subject to change)
func (m *Manager) LeaseSender(sid uuid.UUID) (func(domain.Message) error, func(), error) {
	slog.Default().Debug("lease sender request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan leaseSenderResult, 1)
	m.cmdCh <- leaseSenderCommand{sid: sid}

	r := <-resp

	slog.Default().Info("sender leased", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	return r.sendFunc, r.closeFunc, r.err
}

// ConfigureSession sets the next set of patterns for the session, starting and stopping streams as needed.
// TODO: Replace 'next' parameter with a session configuration struct.
func (m *Manager) ConfigureSession(id uuid.UUID, next []domain.Pattern) error {
	slog.Default().Debug("configure session request", slog.String("cmp", "manager"), slog.String("session", id.String()), slog.Int("patterns", len(next)))
	resp := make(chan configureSessionResult, 1)
	m.cmdCh <- configureSessionCommand{sid: id, next: next, resp: resp}

	r := <-resp

	slog.Default().Info("session configured", slog.String("cmp", "manager"), slog.String("session", id.String()), slog.String("err", fmt.Sprintf("%v", r.err)))
	return r.err
}

// CloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) CloseSession(id uuid.UUID) error {
	slog.Default().Debug("close session request", slog.String("cmp", "manager"), slog.String("session", id.String()))
	resp := make(chan closeSessionResult, 1)
	m.cmdCh <- closeSessionCommand{sid: id, resp: resp}

	r := <-resp

	slog.Default().Info("session closed", slog.String("cmp", "manager"), slog.String("session", id.String()))
	return r.err
}

func (m *Manager) NewWorker(workerType string) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (m *Manager) ConfigureWorker(id uuid.UUID, config any) error {
	return nil
}

func (m *Manager) TerminateWorker(id uuid.UUID) error

// The main loop of the manager, processing commands serially.
func (m *Manager) run() {
	for {
		msg := <-m.cmdCh
		switch c := msg.(type) {
		case createSessionCommand:
			m.handleNewSession(c)
		case leaseReceiverCommand:
			m.handleLeaseReceiever(c)
		case leaseSenderCommand:
			m.handleLeaseSender(c)
		case configureSessionCommand:
			m.handleConfigure(c)
		case closeSessionCommand:
			m.handleCloseSession(c)
		}
	}
}

// Command handlers, run in loop goroutine. With a single goroutine, no locking is needed.

// handleNewSession creates a new session with the given idle timeout. The idle timeout is typically not set by the client, but by the server configuration.
func (m *Manager) handleNewSession(cmd createSessionCommand) {
	s := newSession(time.Second*10, m.router.Incoming())

	// Only arm the idle timer if the timeout is positive. We allow a zero or negative timeout to indicate "never timeout".
	if s.idleAfter <= 0 {
		s.armIdleTimer(func() {
			resp := make(chan closeSessionResult, 1)
			m.cmdCh <- closeSessionCommand{sid: s.id, resp: resp}
			<-resp
		})
	}

	m.sessions[s.id] = s

	cmd.resp <- createSessionResult{sid: s.id}
}

func (m *Manager) handleLeaseReceiever(cmd leaseReceiverCommand) {
	_, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- leaseReceiverResult{nil, nil, ErrSessionNotFound}
	}

	// TODO: Complete lease receiver
}

func (m *Manager) handleLeaseSender(cmd leaseSenderCommand) {
	_, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- leaseSenderResult{nil, nil, ErrSessionNotFound}
	}

	// TODO: Complete lease sender
}

// handleConfigure updates the session bindings, starting and stopping streams as needed. Currently only supports Raw streams.
// TODO: Change this configuration to be an atomic operation, so that partial failures do not end in a half-configured state.
func (m *Manager) handleConfigure(cmd configureSessionCommand) {
	_, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- configureSessionResult{ErrSessionNotFound}
		return
	}

	var errs error
	// TODO: IMPLEMENT!!!

	cmd.resp <- configureSessionResult{err: errs}
}

// handleCloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) handleCloseSession(cmd closeSessionCommand) {
	_, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- closeSessionResult{err: ErrSessionNotFound}
		return
	}

	var errs error
	// TODO: Implement!

	cmd.resp <- closeSessionResult{err: errs}
}
