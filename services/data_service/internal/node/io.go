// Package node
package node

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type In interface {
	Receive() (port string, message domain.Message, ok bool)
}

type Out interface {
	Send(port string, message domain.Message) error
}
