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
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/manager"
)

var (
	ErrWorkerNotRunning = errors.New("worker not running")
	ErrWorkerRunning    = errors.New("worker already running")
)

type Worker interface {
	ID() uuid.UUID
	Start(workerID uuid.UUID, controller manager.SessionController, cfg map[string]string) error
	Stop()
}

type BaseStatefulWorker struct {
	workerUUID uuid.UUID

	sc  manager.SessionController
	sid uuid.UUID
	in  chan<- domain.Message
	out <-chan domain.Message

	running bool
	mu      sync.RWMutex
}

func (w *BaseStatefulWorker) Start(workerID uuid.UUID, sessionController manager.SessionController, _ map[string]string) error {
	if sessionController == nil {
		return errors.New("nil SessionController provided")
	}

	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return ErrWorkerRunning
	}

	sid := sessionController.NewSession(time.Second * 30)
	in, out, err := sessionController.AttachClient(sid, 256, 256)
	if err != nil {
		w.mu.Unlock()
		return err
	}

	w.sc, w.in, w.out = sessionController, in, out
	w.running = true

	w.mu.Unlock()
	return nil
}

func (w *BaseStatefulWorker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}

	err := w.sc.DetachClient(w.sid)
	if err != nil && err != manager.ErrSessionNotFound {
		slog.Default().Error("a non possible error occured", "error", err.Error())
	}

	err = w.sc.CloseSession(w.sid)
	if err != nil && err != manager.ErrSessionNotFound {
		slog.Default().Error("error when closing session", "error", err.Error())
	}

	w.sc, w.in, w.out = nil, nil, nil
	w.workerUUID, w.sid = uuid.Nil, uuid.Nil
	w.running = false

	w.mu.Unlock()
}

func (w *BaseStatefulWorker) ID() uuid.UUID { return w.workerUUID }

func (w *BaseStatefulWorker) Configure(ids []domain.Identifier) error {
	w.mu.RLock()
	if !w.running {
		w.mu.RUnlock()
		return ErrWorkerNotRunning
	}

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
