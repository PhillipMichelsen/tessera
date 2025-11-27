package data

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Ingress interface {
	Name() string
	Configure(cfg map[string]any) error
	Connect(ctx context.Context, actions IngressActions) error
	Disconnect(ctx context.Context) error
}

type IngressActions interface {
	Emit(namespace string, id uuid.UUID, envelope Envelope) error
	EmitBatch(namespace string, id uuid.UUID, envelopes []Envelope) error
}

type LiveIngress interface {
	Subscribe(ctx context.Context, key ...string) error
	Unsubscribe(ctx context.Context, key ...string) error
	ListAvailableKeys(ctx context.Context) []string
	IsValidKey(ctx context.Context, key string) bool
}

type HistoricalIngress interface {
	Fetch(ctx context.Context, key string, start time.Time, end time.Time) ([]Envelope, error)
	ListAvailableKeys(ctx context.Context) []string
	IsValidKey(ctx context.Context, key string) bool
	// some other method that gives the avalible time period for a key
}
