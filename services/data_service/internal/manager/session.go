package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// attachSession wires channels, stops idle timer, and registers ready routes.
// Precondition: session exists and is not attached/closed. Runs in loop.
func (m *Manager) attachSession(s *session, inBuf, outBuf int) (chan<- domain.Message, <-chan domain.Message, error) {
	if inBuf <= 0 {
		inBuf = defaultClientBuf
	}
	if outBuf <= 0 {
		outBuf = defaultClientBuf
	}

	cin := make(chan domain.Message, inBuf)
	cout := make(chan domain.Message, outBuf)
	s.clientIn, s.clientOut = cin, cout

	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}

	// Forward clientIn to router.Incoming with drop on backpressure.
	go func(src <-chan domain.Message, dst chan<- domain.Message) {
		for msg := range src {
			select {
			case dst <- msg:
			default:
				// drop
			}
		}
	}(cin, m.router.IncomingChannel())

	// Register all currently bound that are ready.
	for ident := range s.bound {
		if !ident.IsRaw() {
			m.router.RegisterRoute(ident, cout)
			continue
		}
		// Raw: register only if provider stream is active.
		if p, subj, err := m.resolveProvider(ident); err == nil && p.IsStreamActive(subj) {
			m.router.RegisterRoute(ident, cout)
		}
	}

	s.attached = true
	return cin, cout, nil
}

// detachSession deregisters all routes, closes channels, and arms idle timer.
// Precondition: session exists and is attached. Runs in loop.
func (m *Manager) detachSession(sid uuid.UUID, s *session) error {
	if s.clientOut != nil {
		for ident := range s.bound {
			m.router.DeregisterRoute(ident, s.clientOut)
		}
		close(s.clientOut)
	}
	if s.clientIn != nil {
		close(s.clientIn)
	}
	s.clientIn, s.clientOut = nil, nil
	s.attached = false

	// Arm idle timer to auto-close the session.
	s.idleTimer = time.AfterFunc(s.idleAfter, func() {
		m.cmdCh <- closeSessionCmd{sid: sid, resp: make(chan error, 1)}
	})
	return nil
}

// closeSession performs full teardown and refcount drops. Runs in loop.
func (m *Manager) closeSession(sid uuid.UUID, s *session) {
	if s.closed {
		return
	}
	s.closed = true

	// Detach if attached.
	if s.attached {
		if s.clientOut != nil {
			for ident := range s.bound {
				m.router.DeregisterRoute(ident, s.clientOut)
			}
			close(s.clientOut)
		}
		if s.clientIn != nil {
			close(s.clientIn)
		}
	} else if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}

	// Drop refs for raw identifiers and stop streams if last ref. Fire-and-forget.
	for ident := range s.bound {
		if !ident.IsRaw() {
			continue
		}
		if last := m.decrementStreamRefCount(ident); last {
			if p, subj, err := m.resolveProvider(ident); err == nil {
				_ = p.StopStream(subj) // do not wait
			}
		}
	}

	delete(m.sessions, sid)
}
