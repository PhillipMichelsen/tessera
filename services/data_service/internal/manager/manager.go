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

	workerRegistry      *WorkerRegistry
	workerInstances     map[string]map[string]worker.Worker
	workerUnitRefCounts map[string]map[string]map[string]int
}

// NewManager creates a manager and starts its run loop.
func NewManager(r *router.Router, _ *WorkerRegistry) *Manager {
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
	m.cmdCh <- configureSessionCommand{sid: sid, cfg: cfg, resp: resp}
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

	newCfg, ok := cmd.cfg.(SessionConfig)
	if !ok {
		cmd.resp <- configureSessionResult{err: ErrBadConfig}
		return
	}

	// Normalize workers.
	normalized := make([]WorkerEntry, len(newCfg.Workers))
	for i, we := range newCfg.Workers {
		spec, err := m.workerRegistry.NormalizeSpecificationBytes(we.Type, we.Spec)
		if err != nil {
			cmd.resp <- configureSessionResult{err: err}
			return
		}
		unit, err := m.workerRegistry.NormalizeUnitBytes(we.Type, we.Unit)
		if err != nil {
			cmd.resp <- configureSessionResult{err: err}
			return
		}
		normalized[i] = WorkerEntry{Type: we.Type, Spec: spec, Unit: unit}
	}
	newCfg.Workers = normalized

	// Compute diffs.
	curr := append([]WorkerEntry(nil), s.cfg.Workers...)
	next := append([]WorkerEntry(nil), newCfg.Workers...)
	additions, removals := workerEntryDiffs(curr, next)

	// Per-instance delta: type -> spec -> {add, remove}
	type delta struct{ add, remove [][]byte }
	changes := make(map[string]map[string]delta)
	addTo := func(typ, spec string, u []byte, isAdd bool) {
		if changes[typ] == nil {
			changes[typ] = make(map[string]delta)
		}
		d := changes[typ][spec]
		if isAdd {
			d.add = append(d.add, u)
		} else {
			d.remove = append(d.remove, u)
		}
		changes[typ][spec] = d
	}
	for _, e := range additions {
		addTo(e.Type, string(e.Spec), e.Unit, true)
	}
	for _, e := range removals {
		addTo(e.Type, string(e.Spec), e.Unit, false)
	}

	// Ensure manager maps.
	if m.workerInstances == nil {
		m.workerInstances = make(map[string]map[string]worker.Worker)
	}
	if m.workerUnitRefCounts == nil {
		m.workerUnitRefCounts = make(map[string]map[string]map[string]int)
	}

	// Rollback snapshots.
	type snap struct {
		hadInst bool
		prevRef map[string]int
	}
	snaps := make(map[string]map[string]snap) // type -> spec -> snap
	created := make(map[string]map[string]bool)

	saveSnap := func(typ, spec string) {
		if snaps[typ] == nil {
			snaps[typ] = make(map[string]snap)
		}
		if _, ok := snaps[typ][spec]; ok {
			return
		}
		had := false
		if m.workerInstances[typ] != nil {
			_, had = m.workerInstances[typ][spec]
		}
		prev := make(map[string]int)
		if m.workerUnitRefCounts[typ] != nil && m.workerUnitRefCounts[typ][spec] != nil {
			for k, v := range m.workerUnitRefCounts[typ][spec] {
				prev[k] = v
			}
		}
		snaps[typ][spec] = snap{hadInst: had, prevRef: prev}
	}
	markCreated := func(typ, spec string) {
		if created[typ] == nil {
			created[typ] = make(map[string]bool)
		}
		created[typ][spec] = true
	}

	toBytesSlice := func(ref map[string]int) [][]byte {
		out := make([][]byte, 0, len(ref))
		for k, c := range ref {
			if c > 0 {
				out = append(out, []byte(k))
			}
		}
		return out
	}

	restore := func(err error) {
		// Restore refcounts and instance unit sets.
		for typ, specs := range snaps {
			for spec, sn := range specs {
				// Restore refcounts exactly.
				if m.workerUnitRefCounts[typ] == nil {
					m.workerUnitRefCounts[typ] = make(map[string]map[string]int)
				}
				rc := make(map[string]int)
				for k, v := range sn.prevRef {
					rc[k] = v
				}
				m.workerUnitRefCounts[typ][spec] = rc

				prevUnits := toBytesSlice(rc)

				inst := m.workerInstances[typ][spec]
				switch {
				case sn.hadInst:
					// Ensure instance exists and set units back.
					if inst == nil {
						wi, ierr := m.workerRegistry.Spawn(typ)
						if ierr == nil {
							m.workerInstances[typ][spec] = wi
							inst = wi
							// TODO: pass the correct SessionController
							_ = wi.Start([]byte(spec), s) // best-effort
						}
					}
					if inst != nil {
						_ = inst.SetUnits(prevUnits) // best-effort
					}
				default:
					// We did not have an instance before. Stop and remove if present.
					if inst != nil {
						_ = inst.Stop()
						delete(m.workerInstances[typ], spec)
						if len(m.workerInstances[typ]) == 0 {
							delete(m.workerInstances, typ)
						}
					}
					// If no refs remain, clean refcounts map too.
					if len(rc) == 0 {
						delete(m.workerUnitRefCounts[typ], spec)
						if len(m.workerUnitRefCounts[typ]) == 0 {
							delete(m.workerUnitRefCounts, typ)
						}
					}
				}
			}
		}
		// Clean up instances created during this op that shouldn't exist.
		for typ, specs := range created {
			for spec := range specs {
				if snaps[typ] != nil && snaps[typ][spec].hadInst {
					continue
				}
				if inst := m.workerInstances[typ][spec]; inst != nil {
					_ = inst.Stop()
					delete(m.workerInstances[typ], spec)
					if len(m.workerInstances[typ]) == 0 {
						delete(m.workerInstances, typ)
					}
				}
			}
		}
		cmd.resp <- configureSessionResult{err: err}
	}

	// Apply deltas per instance.
	for typ, specMap := range changes {
		if m.workerUnitRefCounts[typ] == nil {
			m.workerUnitRefCounts[typ] = make(map[string]map[string]int)
		}
		if m.workerInstances[typ] == nil {
			m.workerInstances[typ] = make(map[string]worker.Worker)
		}

		for spec, d := range specMap {
			saveSnap(typ, spec)

			// Update refcounts.
			rc := m.workerUnitRefCounts[typ][spec]
			if rc == nil {
				rc = make(map[string]int)
				m.workerUnitRefCounts[typ][spec] = rc
			}
			for _, u := range d.remove {
				k := string(u)
				if rc[k] > 0 {
					rc[k]--
				}
				if rc[k] == 0 {
					delete(rc, k)
				}
			}
			for _, u := range d.add {
				k := string(u)
				rc[k]++
			}

			desired := toBytesSlice(rc)
			inst := m.workerInstances[typ][spec]

			switch {
			case len(desired) == 0:
				// No units desired: stop and prune if instance exists.
				if inst != nil {
					if err := inst.Stop(); err != nil {
						restore(err)
						return
					}
					delete(m.workerInstances[typ], spec)
					if len(m.workerInstances[typ]) == 0 {
						delete(m.workerInstances, typ)
					}
				}
				// If no refs left, prune refcounts too.
				delete(m.workerUnitRefCounts[typ], spec)
				if len(m.workerUnitRefCounts[typ]) == 0 {
					delete(m.workerUnitRefCounts, typ)
				}

			default:
				// Need instance with desired units.
				if inst == nil {
					wi, err := m.workerRegistry.Instantiate(typ, []byte(spec))
					if err != nil {
						restore(err)
						return
					}
					m.workerInstances[typ][spec] = wi
					markCreated(typ, spec)
					// TODO: pass correct SessionController implementation
					if err := wi.Start([]byte(spec), s); err != nil {
						restore(err)
						return
					}
					inst = wi
				}
				if err := inst.SetUnits(desired); err != nil {
					restore(err)
					return
				}
			}
		}
	}

	// Commit config last.
	if err := s.setConfig(newCfg); err != nil {
		restore(err)
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

	// TODO: Ensure workers are correctly scrapped

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
