// Package router for routing!
package router

import (
	"fmt"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type partition interface {
	registerRoute(domain.Pattern, chan<- domain.Message)
	deregisterRoute(domain.Pattern, chan<- domain.Message)
	deliver(domain.Message)
	start()
	stop()
}

// Factories take a buffer size (internal), router stores a zero-arg thunk.
var partitionFactories = map[string]func(int) partition{
	"actor": func(buf int) partition { return newActorPartition(buf) },
}

type Router struct {
	incoming   chan domain.Message
	mu         sync.RWMutex
	partitions map[string]partition
	newPart    func() partition // zero-arg thunk
	wg         sync.WaitGroup
	started    bool
}

func NewRouter(kind string, incomingBuf, partBuf int) (*Router, error) {
	if incomingBuf <= 0 {
		incomingBuf = 2048
	}
	if partBuf <= 0 {
		partBuf = 1024
	}

	makePartWithBuf, ok := partitionFactories[kind]
	if !ok {
		return nil, fmt.Errorf("unknown partition kind %q", kind)
	}
	// Curry (!!!) to zero-arg
	makePart := func() partition { return makePartWithBuf(partBuf) }

	return &Router{
		incoming:   make(chan domain.Message, incomingBuf),
		partitions: make(map[string]partition),
		newPart:    makePart,
	}, nil
}

func (r *Router) Start() {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()

	r.wg.Go(func() {
		for msg := range r.incoming {
			ns, _, _ := msg.Identifier.Parse()
			r.mu.RLock()
			p, exists := r.partitions[ns]
			r.mu.RUnlock()
			if exists {
				p.deliver(msg)
			}
		}
	})
}

func (r *Router) Stop() {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return
	}
	r.started = false
	close(r.incoming)

	ps := make([]partition, 0, len(r.partitions))
	for _, p := range r.partitions {
		ps = append(ps, p)
	}
	r.partitions = make(map[string]partition)
	r.mu.Unlock()

	for _, p := range ps {
		p.stop()
	}
	r.wg.Wait()
}

func (r *Router) Incoming() chan<- domain.Message { return r.incoming }

func (r *Router) RegisterPattern(pat domain.Pattern, ch chan<- domain.Message) {
	// Inline ensurePartition
	ns := pat.Namespace
	r.mu.RLock()
	p := r.partitions[ns]
	r.mu.RUnlock()
	if p == nil {
		r.mu.Lock()
		// recheck under write lock
		if p = r.partitions[ns]; p == nil {
			p = r.newPart()
			p.start()
			r.partitions[ns] = p
		}
		r.mu.Unlock()
	}
	p.registerRoute(pat, ch)
}

func (r *Router) DeregisterPattern(pat domain.Pattern, ch chan<- domain.Message) {
	r.mu.RLock()
	p := r.partitions[pat.Namespace]
	r.mu.RUnlock()
	if p != nil {
		p.deregisterRoute(pat, ch)
	}
}
