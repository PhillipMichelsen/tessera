// Package data ...
package data

import "time"

type Envelope struct {
	Source    string
	Key       string
	Timestamp time.Time
	Sequence  uint64
	Payload   []byte
}

type Source interface {
	Start() error
	Stop()
	Name() string

	Subscribe(key string) (<-chan Envelope, error)
	Unsubscribe(key string) error

	IsValidKey(key string) bool
	GetSubscriptions() []string
}

type Sink interface {
	Start() error
	Stop()

	Publish(envelope Envelope) error
}

type Processor interface {
	Start(context ProcessorContext) error
	Stop()
}

type ProcessorContext interface {
	Send(inPort string, messsage Envelope) error
	Receive(outPort string) Envelope
}
