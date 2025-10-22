package registry

import (
	"fmt"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/core/data"
)

type SinkFactory func(string) (data.Sink, error)

type Sinks struct {
	mu sync.RWMutex
	m  map[string]SinkFactory
}

func NewSinks() *Sinks { return &Sinks{m: make(map[string]SinkFactory)} }

func (r *Sinks) Register(name string, f SinkFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[name] = f
}

func (r *Sinks) New(name string, params string) (data.Sink, error) {
	r.mu.RLock()
	f, ok := r.m[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown sink: %s", name)
	}
	return f(params)
}
