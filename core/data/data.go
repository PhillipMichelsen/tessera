// Package data ...
package data

import "time"

type Envelope struct {
	Identifier string
	Timestamp  time.Time
	Sequence   uint64
	Payload    []byte
}

type Source interface {
	Start(config any) error
	Stop()
	Name() string

	Subscribe(key string) (<-chan Envelope, error)
	Unsubscribe(key string) error

	IsValidKey(key string) bool
	GetSubscriptions() []string
}

type Processor interface {
	Start(config any, send chan<- Envelope, receive <-chan Envelope) error
	Stop()
}

type Sink interface {
	Start(config any, receive <-chan Envelope) error
	Stop()
}
