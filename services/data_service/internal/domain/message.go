// Package domain contains all key domain types
package domain

type Message struct {
	Identifier Identifier
	Payload    []byte
}
