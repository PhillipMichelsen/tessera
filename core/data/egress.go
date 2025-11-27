package data

import (
	"context"

	"github.com/google/uuid"
)

type Egress interface {
	Name() string
	Configure(cfg map[string]any) error
	Connect(ctx context.Context, actions EgresActions) error
	Disconnect(ctx context.Context) error
}

type EgresActions interface {
	Subscribe(ctx context.Context, namespace string, id uuid.UUID) (<-chan Envelope, func(), error)
}
