// Package manager is the manager package!!!
package manager

import (
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/worker"
)

var ErrSessionNotFound = errors.New("session not found")

// Manager is a single-goroutine actor that owns all state.
type Manager struct {
	cmdCh    chan any
	sessions map[uuid.UUID]*session
	router   *router.Router
}

// NewManager creates a manager and starts its run loop.
func NewManager(r *router.Router, _ *worker.Registry) *Manager {
	m := &Manager{
		cmdCh:    make(chan any, 256),
		sessions: make(map[uuid.UUID]*session),
		router:   r,
	}
	go r.Start()
	go m.run()
	slog.Default().Info("manager started", slog.String("cmp", "manager"))
	return m
}

// API

// CreateSession creates a new session. Arms a 1m idle timer immediately.
func (m *Manager) CreateSession() uuid.UUID {
	slog.Default().Debug("create session request", slog.String("cmp", "manager"))
	resp := make(chan createSessionResult, 1)
	m.cmdCh <- createSessionCommand{resp: resp}
	r := <-resp
	slog.Default().Info("new session created", slog.String("cmp", "manager"), slog.String("session", r.sid.String()))
	return r.sid
}

// LeaseSessionReceiver leases a receiver and returns the receive func and its close func.
func (m *Manager) LeaseSessionReceiver(sid uuid.UUID) (func() (domain.Message, error), error) {
	slog.Default().Debug("lease session receiver request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan leaseSessionReceiverResult, 1)
	m.cmdCh <- leaseSessionReceiverCommand{sid: sid, resp: resp}
	r := <-resp
	return r.receiveFunc, r.err
}

// LeaseSessionSender leases a sender and returns the send func and its close func.
func (m *Manager) LeaseSessionSender(sid uuid.UUID) (func(domain.Message) error, error) {
	slog.Default().Debug("lease sender request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan leaseSessionSenderResult, 1)
	m.cmdCh <- leaseSessionSenderCommand{sid: sid, resp: resp}
	r := <-resp
	return r.sendFunc, r.err
}

// ReleaseSessionReceiver releases the currently held receiver lease
func (m *Manager) ReleaseSessionReceiver(sid uuid.UUID) error {
	slog.Default().Debug("release session receiver request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan releaseSessionReceiverResult, 1)
	m.cmdCh <- releaseSessionReceiverCommand{sid: sid, resp: resp}
	r := <-resp
	return r.err
}

// ReleaseSessionSender releases the currently held receiver lease
func (m *Manager) ReleaseSessionSender(sid uuid.UUID) error {
	slog.Default().Debug("release sender request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan releaseSessionSenderResult, 1)
	m.cmdCh <- releaseSessionSenderCommand{sid: sid, resp: resp}
	r := <-resp
	return r.err
}

// ConfigureSession applies a session config. Pattern wiring left TODO.
func (m *Manager) ConfigureSession(sid uuid.UUID, cfg any) error {
	slog.Default().Debug("configure session request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan configureSessionResult, 1)
	m.cmdCh <- configureSessionCommand{sid: sid, config: cfg, resp: resp}
	r := <-resp
	return r.err
}

// CloseSession closes and removes the session.
func (m *Manager) CloseSession(sid uuid.UUID) error {
	slog.Default().Debug("close session request", slog.String("cmp", "manager"), slog.String("session", sid.String()))
	resp := make(chan closeSessionResult, 1)
	m.cmdCh <- closeSessionCommand{sid: sid, resp: resp}
	r := <-resp
	return r.err
}

func (m *Manager) SpawnWorker(sid uuid.UUID, workerType string) (uuid.UUID, error) {
	slog.Default().Debug("spawn worker request", slog.String("cmp", "manager"), slog.String("session", sid.String()), slog.String("workerType", workerType))
	resp := make(chan spawnWorkerResult, 1)
	m.cmdCh <- spawnWorkerCommand{sid: sid, workerType: workerType, resp: resp}
	r := <-resp
	return r.wid, r.err
}

func (m *Manager) ConfigureWorker(wid uuid.UUID, cfg any) error {
	slog.Default().Debug("configure worker request", slog.String("cmp", "manager"), slog.String("worker", wid.String()))
	resp := make(chan configureWorkerResponse, 1)
	m.cmdCh <- configureWorkerCommand{wid: wid, config: cfg, resp: resp}
	r := <-resp
	return r.err
}

func (m *Manager) TerminateWorker(wid uuid.UUID) error {
	slog.Default().Debug("terminate worker request", slog.String("cmp", "manager"), slog.String("worker", wid.String()))
	resp := make(chan terminateWorkerResult, 1)
	m.cmdCh <- terminateWorkerCommand{wid: wid, resp: resp}
	r := <-resp
	return r.err
}

// --- Loop ---

func (m *Manager) run() {
	for msg := range m.cmdCh {
		switch c := msg.(type) {
		case createSessionCommand:
			m.handleNewSession(c)
		case leaseSessionReceiverCommand:
			m.handleLeaseSessionReceiver(c)
		case leaseSessionSenderCommand:
			m.handleLeaseSessionSender(c)
		case releaseSessionReceiverCommand:
			m.handleReleaseSessionReceiver(c)
		case releaseSessionSenderCommand:
			m.handleReleaseSessionSender(c)
		case configureSessionCommand:
			m.handleConfigureSession(c)
		case closeSessionCommand:
			m.handleCloseSession(c)
		case spawnWorkerCommand:
			m.handleSpawnWorker(c)
		case configureWorkerCommand:
			m.handleConfigureWorker(c)
		case terminateWorkerCommand:
			m.handleTerminateWorker(c)
		}
	}
}

// --- Handlers ---

func (m *Manager) handleNewSession(cmd createSessionCommand) {
	var s *session
	idleCallback := func() { // Generate callback function for the session to be created.
		resp := make(chan closeSessionResult, 1)
		m.cmdCh <- closeSessionCommand{sid: s.id, resp: resp}
		<-resp
	}
	s = newSession(m.router.Incoming(), idleCallback)
	m.sessions[s.id] = s
	cmd.resp <- createSessionResult{sid: s.id}
}

func (m *Manager) handleLeaseSessionReceiver(cmd leaseSessionReceiverCommand) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- leaseSessionReceiverResult{err: ErrSessionNotFound}
		return
	}
	recv, err := s.leaseReceiver()
	if err != nil {
		cmd.resp <- leaseSessionReceiverResult{err: err}
		return
	}

	// Register the patterns and egress channel for the session with the router.
	patterns := s.getPatterns()
	egressChan, ok := s.getEgress()
	if !ok {
		cmd.resp <- leaseSessionReceiverResult{err: errors.New("egress channel doesn't exist despite successful lease")}
	}

	for _, pattern := range patterns {
		m.router.RegisterPattern(pattern, egressChan)
	}

	cmd.resp <- leaseSessionReceiverResult{receiveFunc: recv, err: nil}
}

func (m *Manager) handleLeaseSessionSender(cmd leaseSessionSenderCommand) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- leaseSessionSenderResult{err: ErrSessionNotFound}
		return
	}
	send, err := s.leaseSender()
	if err != nil {
		cmd.resp <- leaseSessionSenderResult{err: err}
		return
	}
	cmd.resp <- leaseSessionSenderResult{sendFunc: send, err: nil}
}

func (m *Manager) handleReleaseSessionReceiver(cmd releaseSessionReceiverCommand) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- releaseSessionReceiverResult{err: ErrSessionNotFound}
		return
	}
	err := s.releaseReceiver()
	if err != nil {
		cmd.resp <- releaseSessionReceiverResult{err: err}
		return
	}
	cmd.resp <- releaseSessionReceiverResult{err: nil}
}

func (m *Manager) handleReleaseSessionSender(cmd releaseSessionSenderCommand) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- releaseSessionSenderResult{err: ErrSessionNotFound}
		return
	}
	err := s.releaseSender()
	if err != nil {
		cmd.resp <- releaseSessionSenderResult{err: err}
		return
	}
	cmd.resp <- releaseSessionSenderResult{err: nil}
}

func (m *Manager) handleConfigureSession(cmd configureSessionCommand) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- configureSessionResult{err: ErrSessionNotFound}
		return
	}

	err := s.changeConfig(cmd.config)
	if err != nil {
		cmd.resp <- configureSessionResult{err: err}
		return
	}

	cmd.resp <- configureSessionResult{err: nil}
}

func (m *Manager) handleCloseSession(cmd closeSessionCommand) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- closeSessionResult{err: ErrSessionNotFound}
		return
	}

	patterns := s.getPatterns()
	egress, ok := s.getEgress()
	if ok { // We only need to deregister if there is an active receiver lease.
		for _, pattern := range patterns {
			m.router.DeregisterPattern(pattern, egress)
		}
	}

	// Release leases and ensure idle timer is disarmed.
	s.closeAll()
	s.disarmIdleTimer()
	delete(m.sessions, cmd.sid)

	cmd.resp <- closeSessionResult{err: nil}
}

func (m *Manager) handleSpawnWorker(cmd spawnWorkerCommand)         {}
func (m *Manager) handleConfigureWorker(cmd configureWorkerCommand) {}
func (m *Manager) handleTerminateWorker(cmd terminateWorkerCommand) {}

