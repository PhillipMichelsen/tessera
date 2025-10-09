package manager

import (
	"errors"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/worker"
)

var (
	ErrWorkerAlreadyRegistered = errors.New("worker type already registered")
	ErrWorkerTypeUnknown       = errors.New("unknown worker type")
	ErrNilFactory              = errors.New("nil worker factory")
	ErrNilNormalizer           = errors.New("nil worker normalizer")
)

type registryEntry struct {
	factory    worker.Factory
	normalizer worker.Normalizer
}

type WorkerRegistry struct {
	mu sync.RWMutex
	m  map[string]registryEntry
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{m: make(map[string]registryEntry)}
}

// Register a worker type with its factory and keyer.
func (wr *WorkerRegistry) Register(workerType string, factory worker.Factory, normalizer worker.Normalizer) error {
	if factory == nil {
		return ErrNilFactory
	}
	if normalizer == nil {
		return ErrNilNormalizer
	}
	wr.mu.Lock()
	defer wr.mu.Unlock()
	if _, ok := wr.m[workerType]; ok {
		return ErrWorkerAlreadyRegistered
	}
	wr.m[workerType] = registryEntry{factory: factory, normalizer: normalizer}
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
	return entry.factory(), nil
}

func (wr *WorkerRegistry) NormalizeSpecificationBytes(workerType string, spec []byte) ([]byte, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.normalizer.NormalizeSpecification(spec)
}

func (wr *WorkerRegistry) NormalizeUnitBytes(workerType string, unit []byte) ([]byte, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.normalizer.NormalizeUnit(unit)
}

// Factory returns the registered factory.
func (wr *WorkerRegistry) Factory(workerType string) (worker.Factory, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.factory, nil
}

func (wr *WorkerRegistry) Normalizer(workerType string) (worker.Normalizer, error) {
	wr.mu.RLock()
	entry, ok := wr.m[workerType]
	wr.mu.RUnlock()
	if !ok {
		return nil, ErrWorkerTypeUnknown
	}
	return entry.normalizer, nil
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
