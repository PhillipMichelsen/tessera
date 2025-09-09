package provider

import "gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"

type Provider interface {
	Start() error
	Stop()

	StartStreams(keys []string) <-chan error
	StopStreams(key []string) <-chan error

	Fetch(key string) (domain.Message, error)

	IsStreamActive(key string) bool
	IsValidSubject(key string, isFetch bool) bool
}
