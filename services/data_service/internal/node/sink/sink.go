// Package sink
package sink

import (
	"context"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/node"
)

type Sink interface {
	Start(ctx context.Context, io IO) error
	Stop()
}

type IO interface {
	node.Sender
}
