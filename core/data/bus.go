package data

import (
	"sync"

	"github.com/google/uuid"
)

type Bus interface {
	Publish(id uuid.UUID, env Envelope) error
	Subscribe(id uuid.UUID) (<-chan Envelope, func(), error)
}

type InMemBus struct {
	mu          sync.RWMutex
	subs        map[uuid.UUID][]chan Envelope
	publishChan chan published
}

type published struct {
	id  uuid.UUID
	env Envelope
}

const (
	busBufferSize        = 256 // buffer size for publishChan
	subscriberBufferSize = 128 // buffer size for each subscriber channel
)

func NewInMemBus() *InMemBus {
	b := &InMemBus{
		subs:        make(map[uuid.UUID][]chan Envelope),
		publishChan: make(chan published, busBufferSize),
	}
	go b.run()
	return b
}

func (b *InMemBus) Publish(id uuid.UUID, env Envelope) error {
	b.publishChan <- published{id: id, env: env}
	return nil
}

func (b *InMemBus) Subscribe(id uuid.UUID) (<-chan Envelope, func(), error) {
	ch := make(chan Envelope, subscriberBufferSize)

	b.mu.Lock()
	b.subs[id] = append(b.subs[id], ch)
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		chans := b.subs[id]
		for i, c := range chans {
			if c == ch {
				// swap-delete
				chans[i] = chans[len(chans)-1]
				chans = chans[:len(chans)-1]
				break
			}
		}

		if len(chans) == 0 {
			delete(b.subs, id)
		} else {
			b.subs[id] = chans
		}
	}

	return ch, cancel, nil
}

func (b *InMemBus) run() {
	for pub := range b.publishChan {
		id := pub.id

		b.mu.RLock()
		chans := b.subs[id]
		for _, ch := range chans {
			select {
			case ch <- pub.env:
			default:
				<-ch
				ch <- pub.env
			}
		}
		b.mu.RUnlock()
	}
}
