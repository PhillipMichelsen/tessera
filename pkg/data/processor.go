package data

import "context"

type Processor interface {
	Name() string
	Configure(cfg map[string]any) error
	Run(ctx context.Context, actions ProcessorActions) error
}

type ProcessorActions interface {
	IngressActions
	EgresActions
}
