package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

const (
	defaultClientBuf = 256
)

// Session holds per-session state. Owned by the manager loop. So we do not need a mutex.
type session struct {
	id uuid.UUID

	inChannel  chan domain.Message // caller writes
	outChannel chan domain.Message // caller reads

	bound map[domain.Identifier]struct{}

	attached  bool
	idleAfter time.Duration
	idleTimer *time.Timer
}

func newSession(idleAfter time.Duration) *session {
	return &session{
		id:        uuid.New(),
		bound:     make(map[domain.Identifier]struct{}),
		attached:  false,
		idleAfter: idleAfter,
	}
}

func (s *session) armIdleTimer(f func()) {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.idleTimer = time.AfterFunc(s.idleAfter, f)
}

func (s *session) disarmIdleTimer() {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
}

// generateNewChannels creates new in/out channels for the session, will not close existing channels.
func (s *session) generateNewChannels(inBuf, outBuf int) (chan domain.Message, chan domain.Message) {
	if inBuf <= 0 {
		inBuf = defaultClientBuf
	}
	if outBuf <= 0 {
		outBuf = defaultClientBuf
	}
	s.inChannel = make(chan domain.Message, inBuf)
	s.outChannel = make(chan domain.Message, outBuf)
	return s.inChannel, s.outChannel
}

// clearChannels closes and nils the in/out channels.
func (s *session) clearChannels() {
	if s.inChannel != nil {
		close(s.inChannel)
		s.inChannel = nil
	}
	if s.outChannel != nil {
		close(s.outChannel)
		s.outChannel = nil
	}
}
