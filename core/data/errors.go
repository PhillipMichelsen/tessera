package data

import "errors"

var (
	ErrUnknownKey        = errors.New("unknown key")
	ErrAlreadySubscribed = errors.New("already subscribed")
	ErrNotSubscribed     = errors.New("not subscribed")
	ErrDroppedMessage    = errors.New("dropped message: buffer full")
)
