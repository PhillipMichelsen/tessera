package router

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/marketdata/internal/identifier"
	"sync"
)

type Message struct {
	Identifier identifier.Identifier
	Payload    any
}

type Router struct {
	incoming <-chan Message
	routes   map[identifier.Identifier][]chan<- Message
	mu       sync.RWMutex
}

func NewRouter(incoming <-chan Message) *Router {
	return &Router{
		incoming: incoming,
		routes:   make(map[identifier.Identifier][]chan<- Message),
	}
}

func (r *Router) Run() {
	for msg := range r.incoming {
		r.mu.RLock()
		chans := r.routes[msg.Identifier]
		for _, ch := range chans {
			ch <- msg
		}
		r.mu.RUnlock()
	}
}

func (r *Router) Register(id identifier.Identifier, ch chan<- Message) {
	r.mu.Lock()
	r.routes[id] = append(r.routes[id], ch)
	r.mu.Unlock()
}

func (r *Router) Deregister(id identifier.Identifier, ch chan<- Message) {
	r.mu.Lock()
	slice := r.routes[id]
	for i := 0; i < len(slice); i++ {
		if slice[i] == ch {
			slice[i] = slice[len(slice)-1]
			slice = slice[:len(slice)-1]
			i--
		}
	}
	if len(slice) == 0 {
		delete(r.routes, id)
	} else {
		r.routes[id] = slice
	}
	r.mu.Unlock()
}
