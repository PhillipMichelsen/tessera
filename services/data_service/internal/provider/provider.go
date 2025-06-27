package provider

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type Provider interface {
	Start() error
	Stop()

	RequestStream(subject string, channel chan domain.Message) error
	CancelStream(subject string)
	GetActiveStreams() []string
	IsStreamActive(subject string) bool

	Fetch(subject string) (domain.Message, error)

	IsValidSubject(subject string, isFetch bool) bool
}
