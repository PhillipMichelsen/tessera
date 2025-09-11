package ws

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type BinanceFutures struct {
	cfg               config
	shards            map[uuid.UUID]*shard
	streamAssignments map[string]*shard
}

type config struct {
	Endpoint           string
	MaxStreamsPerShard uint8
	BatchInterval      time.Duration
}

func NewBinanceFuturesWebsocket(cfg config) *BinanceFutures {
	return &BinanceFutures{
		cfg:    cfg,
		shards: make(map[uuid.UUID]*shard),
	}
}

func (b *BinanceFutures) Start() error {
	return nil
}

func (b *BinanceFutures) Stop() {
	return
}

func (b *BinanceFutures) Subscribe(subject string) <-chan error {
	return nil
}

func (b *BinanceFutures) Unsubscribe(subject string) <-chan error {
	return nil
}

func (b *BinanceFutures) Fetch(subject string) (domain.Message, error) {
	return domain.Message{}, fmt.Errorf("fetch not supported by provider")
}

func (b *BinanceFutures) GetActiveStreams() []string                   { return nil }
func (b *BinanceFutures) IsStreamActive(key string) bool               { return false }
func (b *BinanceFutures) IsValidSubject(key string, isFetch bool) bool { return false }
