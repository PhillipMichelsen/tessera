// Package data ...
package data

import (
	"time"
)

type Envelope struct {
	SendTime time.Time

	Descriptor Descriptor
	Payload    any
}
