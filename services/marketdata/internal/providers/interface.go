package providers

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/marketdata/internal/identifier"
)

type Provider interface {
	Start() error
	Stop() error
	Subscribe(identifier identifier.Identifier, channel chan<- any) error
	Unsubscribe(identifier identifier.Identifier)
	Fetch(identifier identifier.Identifier) (any, error)
}
