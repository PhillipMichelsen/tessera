package worker

import (
	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
)

type Worker interface {
	ID() uuid.UUID
	Start(cfg map[string]string, controller manager.SessionController) error
	Stop()
}
