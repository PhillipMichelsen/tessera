// Package worker provides background processing and task management for the tessera data_service.
// It handles the execution, coordination, and lifecycle of worker routines responsible for data operations.
package worker

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

var (
	ErrWorkerNotRunning = errors.New("worker not running")
	ErrWorkerRunning    = errors.New("worker already running")
)

type (
	ReceiverFunc func() (domain.Message, error)
	SenderFunc   func(m domain.Message) error
)

type SessionController interface {
	CreateSession(idleAfter time.Duration) uuid.UUID

	LeaseSessionReceiver(sid uuid.UUID) (ReceiverFunc, error)
	LeaseSessionSender(sid uuid.UUID) (SenderFunc, error)
	ReleaseSessionReceiver(sid uuid.UUID) error
	ReleaseSessionSender(sid uuid.UUID) error

	ConfigureSession(sid uuid.UUID, cfg any) error
	CloseSession(sid uuid.UUID) error
}

type Worker interface {
	Start(spec []byte, ctrl SessionController) error
	Stop() error
	IsRunning() bool

	SetUnits(units [][]byte) error

	GetSpecification() []byte
	GetUnits() [][]byte
}

type Factory func() Worker

type Keyer interface {
	ComputeSpecificationKey(spec []byte) (string, error)
	ComputeUnitKey(unit []byte) (string, error)
}
