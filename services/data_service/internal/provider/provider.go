package provider

import "gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"

type Provider interface {
	Start() error
	Stop()

	StartStream(key string, destination chan<- domain.Message) <-chan error
	StopStream(key string) <-chan error

	Fetch(key string) (domain.Message, error)

	IsStreamActive(key string) bool
	IsValidSubject(key string, isFetch bool) bool
}
