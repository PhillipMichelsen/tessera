package router

import (
	"slices"
	"sync"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// actorPartition: single goroutine owns state.
type actorPartition struct {
	opCh  chan any
	wg    sync.WaitGroup
	memo  map[domain.Identifier][]chan<- domain.Message
	rules map[string]*ruleEntry // the string is to be a pattern.Canonical()
}

type ruleEntry struct {
	pattern  domain.Pattern
	channels map[chan<- domain.Message]struct{}
}

func newActorPartition(bufferSize int) *actorPartition {
	return &actorPartition{
		opCh:  make(chan any, bufferSize),
		memo:  make(map[domain.Identifier][]chan<- domain.Message),
		rules: make(map[string]*ruleEntry),
	}
}

// External (though not exported) methods to implement the pattern interface

func (p *actorPartition) start() {
	p.wg.Go(p.loop)
}

func (p *actorPartition) stop() {
	close(p.opCh)
	p.wg.Wait()
}

func (p *actorPartition) registerRoute(pat domain.Pattern, ch chan<- domain.Message) {
	done := make(chan struct{}, 1)
	p.opCh <- opRegister{pattern: pat, channel: ch, done: done}
	<-done
}

func (p *actorPartition) deregisterRoute(pat domain.Pattern, ch chan<- domain.Message) {
	done := make(chan struct{}, 1)
	p.opCh <- opDeregister{pattern: pat, channel: ch, done: done}
	<-done
}

func (p *actorPartition) deliver(msg domain.Message) {
	p.opCh <- opDeliver{msg: msg}
}

// Internal

type opRegister struct {
	pattern domain.Pattern
	channel chan<- domain.Message
	done    chan struct{}
}
type opDeregister struct {
	pattern domain.Pattern
	channel chan<- domain.Message
	done    chan struct{}
}
type opDeliver struct{ msg domain.Message }

func (p *actorPartition) loop() {
	for op := range p.opCh {
		switch v := op.(type) {

		case opDeliver:
			id := v.msg.Identifier
			subs, exists := p.memo[id]
			if !exists {
				uniqueChannels := make(map[chan<- domain.Message]struct{})
				for _, e := range p.rules {
					if e.pattern.Match(id) {
						for ch := range e.channels {
							uniqueChannels[ch] = struct{}{}
						}
					}
				}

				if len(uniqueChannels) > 0 {
					uniqueChannelsSlice := make([]chan<- domain.Message, 0, len(uniqueChannels))
					for ch := range uniqueChannels {
						uniqueChannelsSlice = append(uniqueChannelsSlice, ch)
					}
					p.memo[id] = uniqueChannelsSlice
					subs = uniqueChannelsSlice
				} else {
					p.memo[id] = nil // cache "no subscribers", fast hot-path.
					subs = nil
				}
			}

			for _, ch := range subs {
				select {
				case ch <- v.msg:
				default: // drop on full ch
				}
			}

		case opRegister:
			key := v.pattern.Key()
			e, exists := p.rules[key]
			if !exists {
				e = &ruleEntry{pattern: v.pattern, channels: make(map[chan<- domain.Message]struct{})}
				p.rules[key] = e
			}
			if _, exists := e.channels[v.channel]; exists {
				v.done <- struct{}{}
				continue
			}

			e.channels[v.channel] = struct{}{}

			for id, subs := range p.memo {
				if v.pattern.Match(id) && !slices.Contains(subs, v.channel) {
					p.memo[id] = append(subs, v.channel)
				}
			}

			v.done <- struct{}{}

		case opDeregister:
			key := v.pattern.Key()
			e, ok := p.rules[key]
			if !ok {
				v.done <- struct{}{}
				continue
			}
			if _, ok := e.channels[v.channel]; !ok {
				v.done <- struct{}{}
				continue
			}

			delete(e.channels, v.channel)
			if len(e.channels) == 0 {
				delete(p.rules, key)
			}

			for id, subs := range p.memo {
				if v.pattern.Match(id) {
					for i := range subs {
						if subs[i] == v.channel {
							last := len(subs) - 1
							subs[i] = subs[last]
							subs[last] = nil // help GC
							p.memo[id] = subs[:last]
							break
						}
					}
				}
			}

			v.done <- struct{}{}
		}
	}
}
