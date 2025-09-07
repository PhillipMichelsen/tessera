package router

import (
	"fmt"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type Router struct {
	incoming chan domain.Message
	routes   map[domain.Identifier][]chan<- domain.Message
	mu       sync.RWMutex
}

func NewRouter(buffer int) *Router {
	return &Router{
		incoming: make(chan domain.Message, buffer), // Buffered channel for incoming messages
		routes:   make(map[domain.Identifier][]chan<- domain.Message),
	}
}

func (r *Router) IncomingChannel() chan<- domain.Message {
	return r.incoming
}

func (r *Router) Run() {
	for msg := range r.incoming {
		r.mu.RLock()
		channels := r.routes[msg.Identifier]

		for _, ch := range channels {
			select {
			case ch <- msg:
			default:
				fmt.Println("Router could not push message to a full buffer...") // TODO: Handle full buffer case more gracefully
			}
		}
		r.mu.RUnlock()
	}
}

func (r *Router) RegisterRoute(id domain.Identifier, ch chan<- domain.Message) {
	r.mu.Lock()
	r.routes[id] = append(r.routes[id], ch)
	r.mu.Unlock()
}

func (r *Router) DeregisterRoute(id domain.Identifier, ch chan<- domain.Message) {
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
