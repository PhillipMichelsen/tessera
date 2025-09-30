package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// session is manager-owned state and implements SessionIO.
type session struct {
	id        uuid.UUID
	attached  bool
	idleAfter time.Duration
	idleTimer *time.Timer

	ingress  chan<- domain.Message // router.Incoming(); router-owned
	egress   chan domain.Message   // per-lease; owned here
	done     chan struct{}         // per-lease; closed on Release
	released bool                  // guards idempotency inside actor
}

func newSession(idleAfter time.Duration, ingress chan<- domain.Message) *session {
	return &session{
		id:        uuid.New(),
		idleAfter: idleAfter,
		ingress:   ingress,
	}
}

// Lease lifecycle (manager calls)

// openEgress allocates a fresh egress and resets release state.
func (s *session) openEgress(buf int) {
	s.egress = make(chan domain.Message, buf)
	s.done = make(chan struct{})
	s.released = false
}

// closeEgress closes and nils the current egress.
func (s *session) closeEgress() {
	if s.egress != nil {
		close(s.egress)
		s.egress = nil
	}
}

// Idle timer control

func (s *session) armIdleTimer(f func()) {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	if s.idleAfter > 0 {
		s.idleTimer = time.AfterFunc(s.idleAfter, f)
	}
}

func (s *session) disarmIdleTimer() {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
}

func (s *session) Send(m domain.Message) error {
	select {
	case <-s.done:
		return ErrSessionReleased
	case s.ingress <- m:
		return nil
	}
}

func (s *session) Receive() (domain.Message, error) {
	msg, ok := <-s.egress
	if !ok {
		return domain.Message{}, ErrSessionReleased
	}
	return msg, nil
}

func (s *session) Release() {
	if s.released {
		return
	}
	if s.done != nil {
		close(s.done) // stop Send immediately
	}
	s.released = true
}
