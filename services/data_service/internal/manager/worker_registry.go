package manager

import (
	"errors"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/worker"
)

var (
	ErrWorkerTypeExists  = errors.New("worker type already registered")
	ErrWorkerTypeUnknown = errors.New("unknown worker type")
	ErrNilFactory        = errors.New("nil worker factory")
	ErrNilKeyer          = errors.New("nil worker keyer")
)

type registryEntry struct {
	f worker.Factory
	k worker.Keyer
}

type WorkerRegistry struct {
	mu sync.RWMutex
	m  map[string]registryEntry
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{m: make(map[string]registryEntry)}
}

// Register a worker type with its factory and keyer.
func (wr *WorkerRegistry) Register(workerType string, f worker.Factory, k worker.Keyer) error {
	if f == nil {
		return ErrNilFactory
	}
	if k == nil {
		return ErrNilKeyer
	}
	wr.mu.Lock()
	defer wr.mu.Unlock()
	if _, ok := wr.m[workerType]; ok {
		return ErrWorkerTypeExists
	}
	wr.m[workerType] = registryEntry{f: f, k: k}
	return nil
}

// Deregister removes a worker type.
func (wr *WorkerRegistry) Deregister(workerType string) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	if _, ok := wr.m[workerType]; !ok {
		return ErrWorkerTypeUnknown
	}
	delete(wr.m, workerType)
	return nil
}

// Spawn constructs a new worker instance for the given type.
func (wr *WorkerRegistry) Spawn(workerType string) (worker.Worker, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.f(), nil
}

// GetSpecificationKey computes the stable specification key using the type's keyer.
func (wr *WorkerRegistry) GetSpecificationKey(workerType string, spec []byte) (string, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return "", ErrWorkerTypeUnknown
	}
	return entry.k.ComputeSpecificationKey(spec)
}

// GetUnitKey derives the stable unit key using the type's keyer.
func (wr *WorkerRegistry) GetUnitKey(workerType string, unit []byte) (string, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return "", ErrWorkerTypeUnknown
	}
	return entry.k.ComputeUnitKey(unit)
}

// Factory returns the registered factory.
func (wr *WorkerRegistry) Factory(workerType string) (worker.Factory, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.f, nil
}

// Keyer returns the registered keyer.
func (wr *WorkerRegistry) Keyer(workerType string) (worker.Keyer, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.k, nil
}

// RegisteredTypes lists all worker types.
func (wr *WorkerRegistry) RegisteredTypes() []string {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	out := make([]string, 0, len(wr.m))
	for t := range wr.m {
		out = append(out, t)
	}
	return out
}
