package manager

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
)

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

// New creates a manager and starts its run loop.
func New(r *router.Router) *Manager {
	m := &Manager{
		cmdCh:     make(chan any, 256),
		providers: make(map[string]provider.Provider),
		sessions:  make(map[uuid.UUID]*session),
		streamRef: make(map[domain.Identifier]int),
		router:    r,
	}
	go r.Run()
	go m.run()
	return m
}

// Public API (posts commands to loop)

// AddProvider adds and starts a new provider.
func (m *Manager) AddProvider(name string, p provider.Provider) error {
	resp := make(chan error, 1)
	m.cmdCh <- addProviderCmd{name: name, p: p, resp: resp}
	return <-resp
}

// RemoveProvider stops and removes a provider, cleaning up all sessions.
func (m *Manager) RemoveProvider(name string) error {
	resp := make(chan error, 1)
	m.cmdCh <- removeProviderCmd{name: name, resp: resp}
	return <-resp
}

// NewSession creates a new session with the given idle timeout.
func (m *Manager) NewSession(idleAfter time.Duration) (uuid.UUID, error) {
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
	resp := make(chan error, 1)
	m.cmdCh <- detachCmd{sid: id, resp: resp}
	return <-resp
}

// ConfigureSession sets the next set of identifiers for the session, starting and stopping streams as needed.
func (m *Manager) ConfigureSession(id uuid.UUID, next []domain.Identifier) error {
	resp := make(chan error, 1)
	m.cmdCh <- configureCmd{sid: id, next: next, resp: resp}
	return <-resp
}

// CloseSession closes and removes the session, cleaning up all bindings.
func (m *Manager) CloseSession(id uuid.UUID) error {
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
		cmd.resp <- fmt.Errorf("provider exists: %s", cmd.name)
		return
	}
	if err := cmd.p.Start(); err != nil {
		cmd.resp <- fmt.Errorf("start provider %s: %w", cmd.name, err)
		return
	}
	m.providers[cmd.name] = cmd.p
	cmd.resp <- nil
}

func (m *Manager) handleRemoveProvider(cmd removeProviderCmd) {
	p, ok := m.providers[cmd.name]
	if !ok {
		cmd.resp <- fmt.Errorf("provider not found: %s", cmd.name)
		return
	}

	// Clean all identifiers belonging to this provider. Iterates through sessions to reduce provider burden.
	for _, s := range m.sessions {
		for ident := range s.bound {
			provName, subj, ok := ident.ProviderSubject()
			if !ok || provName != cmd.name {
				// TODO: add log warning, but basically should never ever happen
				continue
			}
			if s.attached && s.clientOut != nil {
				m.router.DeregisterRoute(ident, s.clientOut)
			}
			delete(s.bound, ident)

			// decrementStreamRefCount returns true if this was the last ref. In which case we want to stop the stream.
			if ident.IsRaw() && m.decrementStreamRefCount(ident) && subj != "" {
				_ = p.StopStream(subj) // best-effort as we will remove the provider anyway
			}
		}
	}

	// first iteration above is sound, but as a precaution we also clean up any dangling streamRef entries here
	for id := range m.streamRef {
		provName, _, ok := id.ProviderSubject()
		if !ok || provName != cmd.name {
			continue
		}
		fmt.Printf("manager: warning — dangling streamRef for %s after removing provider %s\n", id.Key(), cmd.name)
		delete(m.streamRef, id)
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
}

func (m *Manager) handleConfigure(c configureCmd) {
	s, ok := m.sessions[c.sid]
	if !ok {
		c.resp <- ErrSessionNotFound
		return
	}
	if s.closed {
		c.resp <- ErrSessionClosed
		return
	}

	old := copySet(s.bound)
	toAdd, toDel := identifierSetDifferences(old, c.next)

	// 1) Handle removals first.
	for _, ident := range toDel {
		if s.attached && s.clientOut != nil {
			m.router.DeregisterRoute(ident, s.clientOut)
		}
		delete(s.bound, ident)

		if ident.IsRaw() {
			if m.decrementStreamRefCount(ident) {
				if p, subj, err := m.resolveProvider(ident); err == nil {
					_ = p.StopStream(subj) // fire-and-forget
				}
			}
		}
	}

	// 2) Handle additions. Collect starts to await.
	type startItem struct {
		id domain.Identifier
		ch <-chan error
	}
	var starts []startItem
	var initErrs []error

	for _, ident := range toAdd {
		// Bind intent now.
		s.bound[ident] = struct{}{}

		if !ident.IsRaw() {
			if s.attached && s.clientOut != nil {
				m.router.RegisterRoute(ident, s.clientOut)
			}
			continue
		}

		p, subj, err := m.resolveProvider(ident)
		if err != nil {
			delete(s.bound, ident)
			initErrs = append(initErrs, err)
			continue
		}
		if !p.IsValidSubject(subj, false) {
			delete(s.bound, ident)
			initErrs = append(initErrs, fmt.Errorf("invalid subject %q for provider", subj))
			continue
		}

		first := m.incrementStreamRefCount(ident)

		if first || !p.IsStreamActive(subj) {
			ch := p.StartStream(subj, m.router.IncomingChannel())
			starts = append(starts, startItem{id: ident, ch: ch})
		} else if s.attached && s.clientOut != nil {
			// Already active, just register for this session.
			m.router.RegisterRoute(ident, s.clientOut)
		}
	}

	// 3) Wait for starts initiated by this call, each with its own timeout.
	if len(starts) == 0 {
		c.resp <- join(initErrs)
		return
	}

	type result struct {
		id  domain.Identifier
		err error
	}
	done := make(chan result, len(starts))

	for _, si := range starts {
		// Per-start waiter.
		go func(id domain.Identifier, ch <-chan error) {
			select {
			case err := <-ch:
				done <- result{id: id, err: err}
			case <-time.After(statusWaitTotal):
				done <- result{id: id, err: fmt.Errorf("timeout")}
			}
		}(si.id, si.ch)
	}

	// Collect results and apply.
	for i := 0; i < len(starts); i++ {
		r := <-done
		if r.err != nil {
			// Roll back this session's bind and drop ref.
			delete(s.bound, r.id)
			_ = m.decrementStreamRefCount(r.id)
			initErrs = append(initErrs, fmt.Errorf("start %v: %w", r.id, r.err))
			continue
		}
		// Success: register for any attached sessions that are bound.
		for _, sess := range m.sessions {
			if !sess.attached || sess.clientOut == nil {
				continue
			}
			if _, bound := sess.bound[r.id]; bound {
				m.router.RegisterRoute(r.id, sess.clientOut)
			}
		}
	}

	c.resp <- join(initErrs)
}

func (m *Manager) handleCloseSession(c closeSessionCmd) {
	s, ok := m.sessions[c.sid]
	if !ok {
		c.resp <- ErrSessionNotFound
		return
	}
	m.closeSession(c.sid, s)
	c.resp <- nil
}
