// Package registry ...
package registry

import (
	"fmt"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/core/data"
)

type ProcessorFactory func(string) (data.Processor, error)

type Processors struct {
	mu sync.RWMutex
	m  map[string]ProcessorFactory
}

func NewProcessors() *Processors { return &Processors{m: make(map[string]ProcessorFactory)} }

func (r *Processors) Register(name string, f ProcessorFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[name] = f
}

func (r *Processors) New(name string, params string) (data.Processor, error) {
	r.mu.RLock()
	f, ok := r.m[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown processor: %s", name)
	}
	return f(params)
}
