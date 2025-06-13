package provider

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/marketdata/internal/router"
)

type Provider interface {
	Start() error
	Stop() error

	RequestStream(subject string, channel chan router.Message) error
	CancelStream(subject string)
	GetActiveStreams() []string
	IsStreamActive(subject string) bool

	Fetch(subject string) (router.Message, error)

	IsValidSubject(subject string, isFetch bool) bool
}
