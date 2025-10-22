package registry

import (
	"fmt"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/core/data"
)

type SourceFactory func(string) (data.Source, error)

type Sources struct {
	mu sync.RWMutex
	m  map[string]SourceFactory
}

func NewSources() *Sources { return &Sources{m: make(map[string]SourceFactory)} }

func (r *Sources) Register(name string, f SourceFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[name] = f
}

func (r *Sources) New(name string, params string) (data.Source, error) {
	r.mu.RLock()
	f, ok := r.m[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", name)
	}
	return f(params)
}
