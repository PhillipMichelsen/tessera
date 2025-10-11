// Package node
package node

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type Receiver interface {
	Receive() (port string, message domain.Message, ok bool)
}

type Sender interface {
	Send(port string, message domain.Message) error
}
