package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
)

// Shared constants.
const (
	defaultClientBuf = 256
	statusWaitTotal  = 10 * time.Second
)

// Manager-level errors.
var (
	ErrSessionNotFound       = errorf("session not found")
	ErrSessionClosed         = errorf("session closed")
	ErrClientAlreadyAttached = errorf("client already attached")
	ErrClientNotAttached     = errorf("client not attached")
	ErrInvalidIdentifier     = errorf("invalid identifier")
	ErrUnknownProvider       = errorf("unknown provider")
)

// Commands posted into the manager loop. One struct per action.
type addProviderCmd struct {
	name string
	p    provider.Provider
	resp chan error
}

type removeProviderCmd struct {
	name string
	resp chan error
}

type newSessionCmd struct {
	idleAfter time.Duration
	resp      chan struct {
		id  uuid.UUID
		err error
	}
}

type attachCmd struct {
	sid           uuid.UUID
	inBuf, outBuf int
	resp          chan struct {
		cin  chan<- domain.Message
		cout <-chan domain.Message
		err  error
	}
}

type detachCmd struct {
	sid  uuid.UUID
	resp chan error
}

type configureCmd struct {
	sid  uuid.UUID
	next []domain.Identifier
	resp chan error // returns after starts from this call succeed or timeout
}

type closeSessionCmd struct {
	sid  uuid.UUID
	resp chan error
}
