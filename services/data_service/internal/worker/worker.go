// Package worker provides background processing and task management for the tessera data_service.
// It handles the execution, coordination, and lifecycle of worker routines responsible for data operations.
package worker

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

var (
	ErrWorkerNotRunning = errors.New("worker not running")
	ErrWorkerRunning    = errors.New("worker already running")
)

type SessionController interface {
	NewSession(idleAfter time.Duration) uuid.UUID
	AttachClient(id uuid.UUID, inBuf, outBuf int) (chan<- domain.Message, <-chan domain.Message, error)
	DetachClient(id uuid.UUID) error
	ConfigureSession(id uuid.UUID, next []domain.Identifier) error
	CloseSession(id uuid.UUID) error
}

type Worker interface {
	Start(workerID uuid.UUID, controller SessionController, cfg []byte) error
	Stop() error

	IsRunning() bool
	ID() uuid.UUID
}

type BaseStatefulWorker struct {
	workerUUID uuid.UUID

	sc  SessionController
	sid uuid.UUID
	in  chan<- domain.Message
	out <-chan domain.Message

	running bool
	mu      sync.RWMutex
}

func (w *BaseStatefulWorker) Start(workerUUID uuid.UUID, sessionController SessionController, _ []byte) error {
	if sessionController == nil {
		return errors.New("nil SessionController provided")
	}

	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return ErrWorkerRunning
	}

	sid := sessionController.NewSession(time.Duration(0)) // set a zero duration to disable idle timeout
	in, out, err := sessionController.AttachClient(sid, 256, 256)
	if err != nil {
		w.mu.Unlock()
		return err
	}

	w.sc, w.in, w.out = sessionController, in, out
	w.workerUUID = workerUUID
	w.running = true

	w.mu.Unlock()
	return nil
}

func (w *BaseStatefulWorker) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return ErrWorkerNotRunning
	}

	err := w.sc.DetachClient(w.sid)
	if err != nil {
		slog.Default().Error("error when detaching client", "error", err.Error())
	}

	err = w.sc.CloseSession(w.sid)
	if err != nil {
		slog.Default().Error("error when closing session", "error", err.Error())
	}

	w.sc, w.in, w.out = nil, nil, nil
	w.workerUUID, w.sid = uuid.Nil, uuid.Nil
	w.running = false

	w.mu.Unlock()
	return nil
}

func (w *BaseStatefulWorker) IsRunning() bool {
	w.mu.RLock()
	running := w.running
	w.mu.RUnlock()

	return running
}

func (w *BaseStatefulWorker) ID() uuid.UUID {
	w.mu.RLock()
	id := w.workerUUID
	w.mu.RUnlock()

	return id
}

func (w *BaseStatefulWorker) SetReceiveIdentifiers(ids []domain.Identifier) error {
	w.mu.RLock()
	if !w.running {
		w.mu.RUnlock()
		return ErrWorkerNotRunning
	}

	w.mu.RUnlock()
	return w.sc.ConfigureSession(w.sid, ids)
}

func (w *BaseStatefulWorker) In() chan<- domain.Message {
	w.mu.RLock()
	ch := w.in
	w.mu.RUnlock()

	return ch
}

func (w *BaseStatefulWorker) Out() <-chan domain.Message {
	w.mu.RLock()
	ch := w.out
	w.mu.RUnlock()

	return ch
}
