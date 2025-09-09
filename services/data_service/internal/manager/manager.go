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

func lg() *slog.Logger { return slog.Default().With("cmp", "manager") }

// Manager is a single-goroutine actor that owns all state.
type Manager struct {
	// Command channel
	cmdCh chan any

	// State (loop-owned)
	providers map[string]provider.Provider
	sessions  map[uuid.UUID]*session
	streamRef map[domain.Identifier]int

	// Router
	router *router.Router
}

// NewManager creates a manager and starts its run loop.
func NewManager(r *router.Router) *Manager {
	m := &Manager{
		cmdCh:     make(chan any, 256),
		providers: make(map[string]provider.Provider),
		sessions:  make(map[uuid.UUID]*session),
		streamRef: make(map[domain.Identifier]int),
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
	resp := make(chan error, 1)
	m.cmdCh <- addProviderCmd{name: name, p: p, resp: resp}
	return <-resp
}

// RemoveProvider stops and removes a provider, cleaning up all sessions.
func (m *Manager) RemoveProvider(name string) error {
	lg().Debug("remove provider request", slog.String("name", name))
	resp := make(chan error, 1)
	m.cmdCh <- removeProviderCmd{name: name, resp: resp}
	return <-resp
}

// NewSession creates a new session with the given idle timeout.
func (m *Manager) NewSession(idleAfter time.Duration) (uuid.UUID, error) {
	lg().Debug("new session request", slog.Duration("idle_after", idleAfter))
	resp := make(chan struct {
		id  uuid.UUID
		err error
	}, 1)
	m.cmdCh <- newSessionCmd{idleAfter: idleAfter, resp: resp}
	r := <-resp
	return r.id, r.err
}

// AttachClient attaches a client to a session, creates and returns client channels for the session.
func (m *Manager) AttachClient(id uuid.UUID, inBuf, outBuf int) (chan<- domain.Message, <-chan domain.Message, error) {
	lg().Debug("attach client request", slog.String("session", id.String()), slog.Int("in_buf", inBuf), slog.Int("out_buf", outBuf))
	resp := make(chan struct {
		cin  chan<- domain.Message
		cout <-chan domain.Message
		err  error
	}, 1)
	m.cmdCh <- attachCmd{sid: id, inBuf: inBuf, outBuf: outBuf, resp: resp}
	r := <-resp
	return r.cin, r.cout, r.err
}

// DetachClient detaches the client from the session, closes client channels and arms timeout.
func (m *Manager) DetachClient(id uuid.UUID) error {
	lg().Debug("detach client request", slog.String("session", id.String()))
	resp := make(chan error, 1)
	m.cmdCh <- detachCmd{sid: id, resp: resp}
	return <-resp
}

// ConfigureSession sets the next set of identifiers for the session, starting and stopping streams as needed.
func (m *Manager) ConfigureSession(id uuid.UUID, next []domain.Identifier) error {
	lg().Debug("configure session request", slog.String("session", id.String()), slog.Int("idents", len(next)))
	resp := make(chan error, 1)
	m.cmdCh <- configureCmd{sid: id, next: next, resp: resp}
	return <-resp
}

// CloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) CloseSession(id uuid.UUID) error {
	lg().Debug("close session request", slog.String("session", id.String()))
	resp := make(chan error, 1)
	m.cmdCh <- closeSessionCmd{sid: id, resp: resp}
	return <-resp
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
		cmd.resp <- fmt.Errorf("provider exists: %s", cmd.name)
		return
	}
	if err := cmd.p.Start(); err != nil {
		lg().Warn("failed to start provider", slog.String("name", cmd.name), slog.String("err", err.Error()))
		cmd.resp <- fmt.Errorf("start provider %s: %w", cmd.name, err)
		return
	}
	m.providers[cmd.name] = cmd.p
	cmd.resp <- nil
}

func (m *Manager) handleRemoveProvider(cmd removeProviderCmd) {
	p, ok := m.providers[cmd.name]
	if !ok {
		lg().Warn("provider not found", slog.String("name", cmd.name))
		cmd.resp <- fmt.Errorf("provider not found: %s", cmd.name)
		return
	}

	// Clean all identifiers belonging to this provider. Iterates through sessions to reduce provider burden.
	for _, s := range m.sessions {
		for ident := range s.bound {
			provName, subj, ok := ident.ProviderSubject()
			if !ok || provName != cmd.name {
				// TODO: add log warning, but basically should never ever happen
				lg().Warn("identifier with mismatched provider found in session during provider removal", slog.String("session", s.id.String()), slog.String("ident", ident.Key()), slog.String("expected_provider", cmd.name), slog.String("found_provider", provName))
				continue
			}
			if s.attached && s.clientOut != nil {
				m.router.DeregisterRoute(ident, s.clientOut)
			}
			delete(s.bound, ident)

			// decrementStreamRefCount returns true if this was the last ref. In which case we want to stop the stream.
			if ident.IsRaw() && m.decrementStreamRefCount(ident) && subj != "" {
				_ = p.StopStreams([]string{subj}) // best-effort as we will remove the provider anyway
			}
		}
	}

	// Defensive sweep: log and clear any dangling streamRef entries for this provider.
	for id := range m.streamRef {
		provName, _, ok := id.ProviderSubject()
		if !ok || provName != cmd.name {
			continue
		}
		delete(m.streamRef, id)
		lg().Warn("dangling streamRef entry found during provider removal", slog.String("ident", id.Key()), slog.String("provider", cmd.name))
	}

	p.Stop()
	delete(m.providers, cmd.name)
	cmd.resp <- nil
}

func (m *Manager) handleNewSession(cmd newSessionCmd) {
	s := &session{
		id:        uuid.New(),
		bound:     make(map[domain.Identifier]struct{}),
		idleAfter: cmd.idleAfter,
	}

	// Arm idle timer to auto-close the session.
	s.idleTimer = time.AfterFunc(cmd.idleAfter, func() {
		m.cmdCh <- closeSessionCmd{sid: s.id, resp: make(chan error, 1)}
	})

	m.sessions[s.id] = s // added after arming in the case of immediate timeout or error in arming timer

	cmd.resp <- struct {
		id  uuid.UUID
		err error
	}{id: s.id, err: nil}

	lg().Info("new session created", slog.String("session", s.id.String()), slog.Duration("idle_after", cmd.idleAfter))
}

func (m *Manager) handleAttach(cmd attachCmd) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- struct {
			cin  chan<- domain.Message
			cout <-chan domain.Message
			err  error
		}{nil, nil, ErrSessionNotFound}
		return
	}
	if s.closed {
		cmd.resp <- struct {
			cin  chan<- domain.Message
			cout <-chan domain.Message
			err  error
		}{nil, nil, ErrSessionClosed}
		return
	}
	if s.attached {
		cmd.resp <- struct {
			cin  chan<- domain.Message
			cout <-chan domain.Message
			err  error
		}{nil, nil, ErrClientAlreadyAttached}
		return
	}

	cin, cout, err := m.attachSession(s, cmd.inBuf, cmd.outBuf)

	cmd.resp <- struct {
		cin  chan<- domain.Message
		cout <-chan domain.Message
		err  error
	}{cin, cout, err}

	lg().Info("client attached to session", slog.String("session", s.id.String()))
}

func (m *Manager) handleDetach(cmd detachCmd) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- ErrSessionNotFound
		return
	}
	if s.closed {
		cmd.resp <- ErrSessionClosed
		return
	}
	if !s.attached {
		cmd.resp <- ErrClientNotAttached
		return
	}

	_ = m.detachSession(cmd.sid, s)

	cmd.resp <- nil

	lg().Info("client detached from session", slog.String("session", s.id.String()))
}

func (m *Manager) handleConfigure(cmd configureCmd) {
	s, ok := m.sessions[cmd.sid]
	if !ok {
		cmd.resp <- ErrSessionNotFound
		return
	}
	if s.closed {
		cmd.resp <- ErrSessionClosed
		return
	}

	old := copySet(s.bound)
	toAdd, toDel := identifierSetDifferences(old, cmd.next)

	var aggErrs error

	// 1) Build batches: provider → starts(starters) and stops(subjects)
	type starter struct {
		id   domain.Identifier
		subj string
	}
	startsByProv := make(map[provider.Provider][]starter)
	stopsByProv := make(map[provider.Provider][]string)

	// Removals
	for _, ident := range toDel {
		if s.attached && s.clientOut != nil {
			m.router.DeregisterRoute(ident, s.clientOut)
		}
		delete(s.bound, ident)

		if !ident.IsRaw() {
			continue
		}

		p, subj, err := m.resolveProvider(ident)
		if err != nil {
			aggErrs = errors.Join(aggErrs, fmt.Errorf("stop %s: %w", ident.Key(), err))
			continue
		}
		if subj == "" {
			continue
		}

		if m.decrementStreamRefCount(ident) { // only when last ref
			stopsByProv[p] = append(stopsByProv[p], subj)
		}
	}

	// Additions
	for _, ident := range toAdd {
		if !ident.IsRaw() {
			if s.attached && s.clientOut != nil {
				m.router.RegisterRoute(ident, s.clientOut)
			}
			s.bound[ident] = struct{}{}
			continue
		}

		p, subj, err := m.resolveProvider(ident)
		if err != nil {
			aggErrs = errors.Join(aggErrs, err)
			continue
		}
		if !p.IsValidSubject(subj, false) {
			aggErrs = errors.Join(aggErrs, fmt.Errorf("invalid subject %q", subj))
			continue
		}

		if m.incrementStreamRefCount(ident) { // first ref → start later
			startsByProv[p] = append(startsByProv[p], starter{id: ident, subj: subj})
		} else {
			// already active → bind+route now
			if s.attached && s.clientOut != nil {
				m.router.RegisterRoute(ident, s.clientOut)
			}
			s.bound[ident] = struct{}{}
		}
	}

	// 2) Fire provider calls
	type batchRes struct {
		prov provider.Provider
		err  error
		op   string // "start"/"stop"
	}
	done := make(chan batchRes, len(startsByProv)+len(stopsByProv))

	// Start batches
	for p, items := range startsByProv {
		subjs := make([]string, 0, len(items))
		for _, it := range items {
			subjs = append(subjs, it.subj)
		}
		ack := p.StartStreams(subjs)
		go func(p provider.Provider, ack <-chan error) {
			var err error
			select {
			case err = <-ack:
			case <-time.After(statusWaitTotal):
				err = fmt.Errorf("timeout")
			}
			done <- batchRes{prov: p, err: err, op: "start"}
		}(p, ack)
	}

	// Stop batches
	for p, subjs := range stopsByProv {
		ack := p.StopStreams(subjs)
		go func(p provider.Provider, ack <-chan error) {
			var err error
			select {
			case err = <-ack:
			case <-time.After(statusWaitTotal):
				err = fmt.Errorf("timeout")
			}
			done <- batchRes{prov: p, err: err, op: "stop"}
		}(p, ack)
	}

	// 3) Collect results
	for i := 0; i < len(startsByProv)+len(stopsByProv); i++ {
		r := <-done
		switch r.op {
		case "start":
			items := startsByProv[r.prov]
			if r.err != nil {
				// Roll back refcounts for each ident in this provider batch
				for _, it := range items {
					_ = m.decrementStreamRefCount(it.id)
					aggErrs = errors.Join(aggErrs, fmt.Errorf("start %s: %w", it.id.Key(), r.err))
				}
				continue
			}
			// Success → bind and route
			for _, it := range items {
				if s.attached && s.clientOut != nil {
					m.router.RegisterRoute(it.id, s.clientOut)
				}
				s.bound[it.id] = struct{}{}
			}
		case "stop":
			if r.err != nil {
				for _, subj := range stopsByProv[r.prov] {
					aggErrs = errors.Join(aggErrs, fmt.Errorf("stop %s/%s: %w", "raw", subj, r.err))
				}
			}
		}
	}

	cmd.resp <- aggErrs

	lg().Info("session configured", slog.String("session", s.id.String()), slog.Int("bound", len(s.bound)), slog.Int("to_add", len(toAdd)), slog.Int("to_del", len(toDel)))
}

func (m *Manager) handleCloseSession(c closeSessionCmd) {
	s, ok := m.sessions[c.sid]
	if !ok {
		c.resp <- ErrSessionNotFound
		return
	}
	m.closeSession(c.sid, s)
	c.resp <- nil

	lg().Info("session closed", slog.String("session", s.id.String()))
}
