package manager

import (
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

var (
	// Lease lifecycle errors.
	ErrAlreadyReleased       = errors.New("lease already released")
	ErrSenderAlreadyLeased   = errors.New("sender already leased")
	ErrReceiverAlreadyLeased = errors.New("receiver already leased")

	// Config errors (enforced by manager).
	ErrConfigActiveLeases = errors.New("cannot configure while a lease is active")
)

// SessionConfig carries non-live-tunable knobs for a session.
// Manager mutates this directly; session does not expose Configure anymore.
type SessionConfig struct {
	IdleAfter    time.Duration // <=0 disables idle timer
	EgressBuffer int           // receiver egress buffer size
	Patterns     []domain.Pattern
}

// session is manager-owned state. Single goroutine access.
type session struct {
	id uuid.UUID

	// Router pipes
	ingress chan<- domain.Message // router.Incoming(); router-owned
	egress  chan domain.Message   // current receiver lease egress; owned here

	// Config and timers
	cfg          SessionConfig
	idleTimer    *time.Timer
	idleCallback func() // stored on creation

	// Sender lease
	sendOpen bool
	sendDone chan struct{}

	// Receiver lease
	receiveOpen bool
	receiveDone chan struct{}
}

// newSession arms a 1-minute idle timer immediately. Manager must
// configure before it fires. idleCb is invoked by the timer.
func newSession(ingress chan<- domain.Message, idleCb func()) *session {
	s := &session{
		id:      uuid.New(),
		ingress: ingress,
		cfg: SessionConfig{
			IdleAfter:    time.Minute, // default 1m on creation
			EgressBuffer: 256,         // default buffer
		},
		idleCallback: idleCb,
	}
	s.armIdleTimer()
	return s
}

func normalizeConfig(in SessionConfig) SessionConfig {
	out := in
	if out.EgressBuffer <= 0 {
		out.EgressBuffer = 256
	}
	return out
}

// leaseSend opens a sender lease and returns (send, close, err).
func (s *session) leaseSend() (func(domain.Message) error, func(), error) {
	if s.sendOpen {
		return nil, nil, ErrSenderAlreadyLeased
	}
	s.sendOpen = true
	s.sendDone = make(chan struct{})
	s.disarmIdleTimer()

	// Capture lease-scoped handle
	done := s.sendDone

	sendFunc := func(m domain.Message) error {
		select {
		case <-done:
			return ErrAlreadyReleased
		case s.ingress <- m:
			return nil
		}
	}

	releaseFunc := func() {
		if !s.sendOpen || s.sendDone != done {
			return
		}
		s.sendOpen = false
		close(done)
		s.sendDone = nil

		if !s.sendOpen && !s.receiveOpen {
			s.armIdleTimer()
		}
	}

	return sendFunc, releaseFunc, nil
}

// leaseReceive opens a receiver lease and returns (receive, close, err).
func (s *session) leaseReceive() (func() (domain.Message, error), func(), error) {
	if s.receiveOpen {
		return nil, nil, ErrReceiverAlreadyLeased
	}
	s.receiveOpen = true
	s.receiveDone = make(chan struct{})
	s.egress = make(chan domain.Message, s.cfg.EgressBuffer)
	s.disarmIdleTimer()

	// Capture lease-scoped handles
	done := s.receiveDone
	eg := s.egress

	receiveFunc := func() (domain.Message, error) {
		select {
		case <-done:
			return domain.Message{}, ErrAlreadyReleased
		case msg, ok := <-eg:
			if !ok {
				return domain.Message{}, ErrAlreadyReleased
			}
			return msg, nil
		}
	}

	releaseFunc := func() {
		if !s.receiveOpen || s.receiveDone != done || s.egress != eg {
			return
		}
		s.receiveOpen = false
		close(done)
		close(eg)
		s.receiveDone = nil
		s.egress = nil

		if !s.sendOpen && !s.receiveOpen {
			s.armIdleTimer()
		}
	}

	return receiveFunc, releaseFunc, nil
}

// closeAll force-releases both sender and receiver leases. Safe to call multiple times.
// Manager must stop any routing into s.egress before calling this. Or more generally, no more messages into s.egress before calling.
func (s *session) closeAll() {
	// Sender
	if s.sendOpen {
		s.sendOpen = false
		if s.sendDone != nil {
			close(s.sendDone)
			s.sendDone = nil
		}
	}
	// Receiver
	if s.receiveOpen {
		s.receiveOpen = false
		if s.receiveDone != nil {
			close(s.receiveDone)
			s.receiveDone = nil
		}
		if s.egress != nil {
			close(s.egress)
			s.egress = nil
		}
	}
}

// armIdleTimer arms a timer using stored cfg.IdleAfter and idleCb.
func (s *session) armIdleTimer() {
	if s.idleCallback == nil {
		slog.Warn("nil idle callback function provided to session")
	}
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
	if s.cfg.IdleAfter > 0 && s.idleCallback != nil {
		s.idleTimer = time.AfterFunc(s.cfg.IdleAfter, s.idleCallback)
	}
}

// disarmIdleTimer disarms the idle timer if active.
func (s *session) disarmIdleTimer() {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
}
