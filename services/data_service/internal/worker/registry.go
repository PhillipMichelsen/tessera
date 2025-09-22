package worker

import (
	"fmt"
	"sync"
)

type Factory func() Worker

type Registry struct {
	mu              sync.RWMutex
	workerFactories map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{
		workerFactories: make(map[string]Factory),
	}
}

func (r *Registry) Register(workerType string, workerFactory Factory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.workerFactories[workerType]; ok {
		return fmt.Errorf("worker already registered: %s", workerType)
	}

	if workerFactory == nil {
		return fmt.Errorf("nil workerFactory provided for: %s", workerType)
	}

	r.workerFactories[workerType] = workerFactory

	return nil
}

func (r *Registry) Spawn(workerType string) (Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workerFactory, ok := r.workerFactories[workerType]
	if !ok {
		return nil, fmt.Errorf("unknown worker type: %s", workerType)
	}

	return workerFactory(), nil
}

func (r *Registry) RegisteredWorkers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workerTypes := make([]string, 0, len(r.workerFactories))
	for k := range r.workerFactories {
		workerTypes = append(workerTypes, k)
	}

	return workerTypes
}

func (r *Registry) Factory(workerType string) (Factory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workerFactory, ok := r.workerFactories[workerType]
	return workerFactory, ok
}
