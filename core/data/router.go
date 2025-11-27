package data

import "github.com/google/uuid"

type Router struct {
	busses map[string]Bus
}

func NewRouter() *Router {
	return &Router{
		busses: make(map[string]Bus),
	}
}

func (r *Router) Emit(namespace string, id uuid.UUID, envelope Envelope) error         { return nil }
func (r *Router) EmitBatch(namespace string, id uuid.UUID, enveloeps []Envelope) error { return nil }
