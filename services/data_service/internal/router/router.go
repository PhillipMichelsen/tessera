// Package router
package router

import "github.com/google/uuid"

type Router interface {
	Route(fromNode uuid.UUID, fromOutPort string, toNode uuid.UUID, toInPort string)
	Unroute(fromNode uuid.UUID, fromOutPort string, toNode uuid.UUID, toInPort string)
}
