// Package source
package source

import (
	"context"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/node"
)

type Source interface {
	Start(ctx context.Context, cfg string, io IO) error
	Stop()

	Serve(key string) error
	Unserve(key string) error
}

type IO interface {
	node.Sender
}
