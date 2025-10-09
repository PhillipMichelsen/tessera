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
	ErrSenderNotLeased       = errors.New("no sender lease active")
	ErrReceiverNotLeased     = errors.New("no receiver lease active")

	// Config errors
	ErrBadConfig          = errors.New("config not valid")
	ErrConfigActiveLeases = errors.New("cannot configure while a lease is active")
)

type WorkerEntry struct {
	Type string
	Spec []byte
	Unit []byte
}

// SessionConfig carries non-live-tunable knobs for a session.
// Manager mutates this directly; session does not expose Configure anymore.
type SessionConfig struct {
	IdleAfter    time.Duration // <=0 disables idle timer
	EgressBuffer int           // receiver egress buffer size
	Patterns     []domain.Pattern
	Workers      []WorkerEntry
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

func (s *session) setConfig(cfg any) error {
	if s.sendOpen || s.receiveOpen {
		return ErrConfigActiveLeases
	}

	cfgParsed, ok := cfg.(SessionConfig)
	if !ok {
		return ErrBadConfig
	}

	s.cfg = cfgParsed

	return nil
}

func (s *session) getEgress() (chan<- domain.Message, bool) {
	if s.egress == nil {
		return nil, false
	}
	return s.egress, true
}

func (s *session) getPatterns() []domain.Pattern {
	return nil
}

// leaseSender opens a sender lease and returns send(m) error.
func (s *session) leaseSender() (func(domain.Message) error, error) {
	if s.sendOpen {
		return nil, ErrSenderAlreadyLeased
	}
	s.sendOpen = true
	s.sendDone = make(chan struct{})
	s.disarmIdleTimer()

	// Snapshot for lease-scoped handle.
	done := s.sendDone

	sendFunc := func(m domain.Message) error {
		select {
		case <-done:
			return ErrAlreadyReleased
		case s.ingress <- m:
			return nil
		}
	}

	return sendFunc, nil
}

// releaseSender releases the current sender lease.
func (s *session) releaseSender() error {
	if !s.sendOpen {
		return ErrSenderNotLeased
	}
	s.sendOpen = false
	if s.sendDone != nil {
		close(s.sendDone) // invalidates all prior send funcs
		s.sendDone = nil
	}
	if !s.receiveOpen {
		s.armIdleTimer()
	}
	return nil
}

// leaseReceiver opens a receiver lease and returns receive() (Message, error).
func (s *session) leaseReceiver() (func() (domain.Message, error), error) {
	if s.receiveOpen {
		return nil, ErrReceiverAlreadyLeased
	}
	s.receiveOpen = true
	s.receiveDone = make(chan struct{})
	s.egress = make(chan domain.Message, s.cfg.EgressBuffer)
	s.disarmIdleTimer()

	// Snapshots for lease-scoped handles.
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

	return receiveFunc, nil
}

// releaseReceiver releases the current receiver lease.
// Manager must stop any routing into s.egress before calling this.
func (s *session) releaseReceiver() error {
	if !s.receiveOpen {
		return ErrReceiverNotLeased
	}
	s.receiveOpen = false
	if s.receiveDone != nil {
		close(s.receiveDone) // invalidates all prior receive funcs
		s.receiveDone = nil
	}
	if s.egress != nil {
		close(s.egress)
		s.egress = nil
	}
	if !s.sendOpen {
		s.armIdleTimer()
	}
	return nil
}

// closeAll force-releases both sender and receiver leases. Safe to call multiple times.
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
