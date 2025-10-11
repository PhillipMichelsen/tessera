// Package control
package control

import (
	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/node"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/node/processor"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/router"
)

type nodeEntry struct {
	Template            node.Template
	TemplateFingerprint string
}

type Controller struct {
	router            router.Router
	sourceRegistry    any // source.Registry
	processorRegistry processor.Registry
	sinkRegistry      any // sink.Registry

	sourceNodes    map[uuid.UUID]any
	processorNodes map[uuid.UUID]processor.Processor
	sinkNodes      map[uuid.UUID]any
}
