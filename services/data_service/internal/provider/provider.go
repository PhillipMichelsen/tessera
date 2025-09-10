package provider

import "gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"

type Provider interface {
	Start() error
	Stop()

	Subscribe(subject string) <-chan error
	Unsubscribe(subject string) <-chan error
	Fetch(subject string) (domain.Message, error)

	GetActiveStreams() []string
	IsStreamActive(key string) bool
	IsValidSubject(key string, isFetch bool) bool
}
