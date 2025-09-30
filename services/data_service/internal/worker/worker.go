// Package worker provides background processing and task management for the tessera data_service.
// It handles the execution, coordination, and lifecycle of worker routines responsible for data operations.
package worker

import (
	"context"
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
	CloseFunc    func()
)

type SessionController interface {
	CreateSession(idleAfter time.Duration) uuid.UUID
	LeaseSender(sid uuid.UUID) (SenderFunc, CloseFunc, error)
	LeaseReceiver(sid uuid.UUID) (ReceiverFunc, CloseFunc, error)
	ConfigureSession(sid uuid.UUID, next []domain.Identifier) error
	CloseSession(sid uuid.UUID) error
}

type Instruction struct{}

type Worker interface {
	Start(wid uuid.UUID, controller SessionController, cfg []byte) error
	Stop() error

	Execute(ctx context.Context, inst Instruction) error

	IsRunning() bool
	ID() uuid.UUID
}
