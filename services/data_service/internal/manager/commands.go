package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// Commands posted into the manager loop. One struct per action.
type newSessionCmd struct {
	idleAfter time.Duration
	resp      chan newSessionResult
}

type newSessionResult struct {
	id uuid.UUID
}

type attachCmd struct {
	sid           uuid.UUID
	inBuf, outBuf int
	resp          chan attachResult
}

type attachResult struct {
	cin  chan<- domain.Message
	cout <-chan domain.Message
	err  error
}

type detachCmd struct {
	sid  uuid.UUID
	resp chan detachResult
}

type detachResult struct {
	err error
}

type configureCmd struct {
	sid  uuid.UUID
	next []domain.Pattern
	resp chan configureResult
}

type configureResult struct {
	err error
}

type closeSessionCmd struct {
	sid  uuid.UUID
	resp chan closeSessionResult
}

type closeSessionResult struct {
	err error
}
