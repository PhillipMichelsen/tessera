package manager

import (
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
)

type SessionController interface {
	NewSession(idleAfter time.Duration) uuid.UUID
	AttachClient(id uuid.UUID, inBuf, outBuf int) (chan<- domain.Message, <-chan domain.Message, error)
	DetachClient(id uuid.UUID) error
	ConfigureSession(id uuid.UUID, next []domain.Identifier) error
	CloseSession(id uuid.UUID) error
}

type ProviderController interface {
	AddProvider(name string, p provider.Provider) error
	RemoveProvider(name string) error
}
