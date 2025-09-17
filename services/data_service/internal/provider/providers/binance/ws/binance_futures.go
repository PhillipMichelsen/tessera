package ws

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

const providerName = "binance_futures"

type Config struct {
	Endpoint           string
	MaxStreamsPerShard uint16
	RateLimitPerSec    uint16
}

type BinanceFutures struct {
	cfg Config
	bus chan<- domain.Message

	mu                sync.RWMutex
	shards            map[uuid.UUID]*shard
	assignOrder       []uuid.UUID
	streamAssignments map[string]*shard
	pendingGlobal     map[string][]chan error

	ctx    context.Context
	cancel context.CancelFunc

	idSeq atomic.Uint64
}

func NewBinanceFuturesWebsocket(cfg Config, bus chan<- domain.Message) *BinanceFutures {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "wss://fstream.binance.com/stream"
	}
	if cfg.RateLimitPerSec <= 0 {
		cfg.RateLimitPerSec = 5
	}
	if cfg.MaxStreamsPerShard == 0 {
		cfg.MaxStreamsPerShard = 15
	}
	return &BinanceFutures{
		cfg:               cfg,
		bus:               bus,
		shards:            make(map[uuid.UUID]*shard),
		streamAssignments: make(map[string]*shard),
		pendingGlobal:     make(map[string][]chan error),
	}
}

func (b *BinanceFutures) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx != nil {
		return nil
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	slog.Default().Info("started", slog.String("cmp", providerName))
	sh, err := newShard(b.ctx, b.cfg, b.bus, b.nextReqID)
	if err != nil {
		slog.Default().Error("", "error", err)
		return err
	}
	b.shards[sh.ID] = sh
	b.assignOrder = []uuid.UUID{sh.ID}

	// idle shard GC
	go b.gcIdleShards()

	return nil
}

func (b *BinanceFutures) Stop() {
	b.mu.Lock()
	if b.cancel != nil {
		b.cancel()
	}
	// snapshot shards, then clear maps
	shs := make([]*shard, 0, len(b.shards))
	for _, sh := range b.shards {
		shs = append(shs, sh)
	}
	b.shards = map[uuid.UUID]*shard{}
	b.assignOrder = nil
	b.streamAssignments = map[string]*shard{}

	for subj, waiters := range b.pendingGlobal {
		for _, ch := range waiters {
			select {
			case ch <- context.Canceled:
			default:
			}
		}
		delete(b.pendingGlobal, subj)
	}
	slog.Default().Info("stopped", slog.String("cmp", providerName))
	b.mu.Unlock()

	for _, sh := range shs {
		sh.stop()
	}
}

func (b *BinanceFutures) Subscribe(subject string) <-chan error {
	ch := make(chan error, 1)
	if !IsValidSubject(subject) {
		ch <- fmt.Errorf("invalid subject: %s", subject)
		return ch
	}

	b.mu.Lock()
	if sh, ok := b.streamAssignments[subject]; ok && sh.isActive(subject) {
		b.mu.Unlock()
		ch <- nil
		return ch
	}
	sh := b.pickShardLocked()
	b.streamAssignments[subject] = sh
	sh.enqueueSubscribe(subject, ch)
	b.mu.Unlock()
	return ch
}

func (b *BinanceFutures) Unsubscribe(subject string) <-chan error {
	ch := make(chan error, 1)

	b.mu.Lock()
	sh, ok := b.streamAssignments[subject]
	if ok {
		delete(b.streamAssignments, subject) // allow reassignment later
	}
	b.mu.Unlock()

	if !ok {
		ch <- nil
		return ch
	}
	sh.enqueueUnsubscribe(subject, ch)
	return ch
}

func (b *BinanceFutures) Fetch(_ string) (domain.Message, error) {
	return domain.Message{}, fmt.Errorf("fetch not supported by provider")
}

func (b *BinanceFutures) GetActiveStreams() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0)
	for _, sh := range b.shards {
		out = append(out, sh.activeList()...)
	}
	return out
}

func (b *BinanceFutures) IsStreamActive(key string) bool {
	b.mu.RLock()
	sh := b.streamAssignments[key]
	b.mu.RUnlock()
	if sh == nil {
		return false
	}
	return sh.isActive(key)
}

func (b *BinanceFutures) IsValidSubject(key string, _ bool) bool { return IsValidSubject(key) }

// pick shard by lowest load = active + pending subs; enforce cap
func (b *BinanceFutures) pickShardLocked() *shard {
	var chosen *shard
	minLoad := int(^uint(0) >> 1) // max int

	for _, id := range b.assignOrder {
		sh := b.shards[id]
		if sh == nil {
			continue
		}
		load := sh.loadEstimate()
		if load < int(b.cfg.MaxStreamsPerShard) && load < minLoad {
			minLoad = load
			chosen = sh
		}
	}
	if chosen != nil {
		return chosen
	}

	// need a new shard
	sh, err := newShard(b.ctx, b.cfg, b.bus, b.nextReqID)
	if err != nil {
		if len(b.assignOrder) > 0 {
			return b.shards[b.assignOrder[0]]
		}
		return sh
	}
	b.shards[sh.ID] = sh
	b.assignOrder = append(b.assignOrder, sh.ID)
	return sh
}

func (b *BinanceFutures) nextReqID() uint64 { return b.idSeq.Add(1) }

// Close idle shards periodically. Keep at least one.
func (b *BinanceFutures) gcIdleShards() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			var toStop []*shard

			b.mu.Lock()
			if len(b.shards) <= 1 {
				b.mu.Unlock()
				continue
			}
			for id, sh := range b.shards {
				if len(b.shards)-len(toStop) <= 1 {
					break // keep one
				}
				if sh.isIdle() {
					toStop = append(toStop, sh)
					delete(b.shards, id)
					// prune order list
					for i, v := range b.assignOrder {
						if v == id {
							b.assignOrder = append(b.assignOrder[:i], b.assignOrder[i+1:]...)
							break
						}
					}
				}
			}
			b.mu.Unlock()

			for _, sh := range toStop {
				slog.Default().Info("close idle shard", "cmp", providerName, "shard", sh.ID)
				sh.stop()
			}
		}
	}
}
