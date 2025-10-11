// Package domain
package domain

import "time"

type Message struct {
	Payload   []byte
	SchemaID  string
	Seq       uint64
	Timestamp time.Time
}
