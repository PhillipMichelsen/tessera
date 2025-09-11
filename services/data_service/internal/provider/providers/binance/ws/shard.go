package ws

import (
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type shard struct {
	ID            uuid.UUID
	conn          websocket.Conn
	activeStreams []string
}
