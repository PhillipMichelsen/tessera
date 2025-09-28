package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// Commands posted into the manager loop. One struct per action.
type createSessionCommand struct {
	idleAfter time.Duration
	resp      chan createSessionResult
}

type createSessionResult struct {
	sid uuid.UUID
}

type aquireSessionIOCommand struct {
	sid  uuid.UUID
	resp chan aquireSessionIOCommand
}

type aquireSessionIOResult struct {
	sessionIO SessionIO
	err       error
}

type configureSessionCommand struct {
	sid  uuid.UUID
	next []domain.Pattern
	resp chan configureSessionResult
}

type configureSessionResult struct {
	err error
}

type closeSessionCommand struct {
	sid  uuid.UUID
	resp chan closeSessionResult
}

type closeSessionResult struct {
	err error
}
