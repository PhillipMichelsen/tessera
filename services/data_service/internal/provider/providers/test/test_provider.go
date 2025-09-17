// Package test implements a configurable synthetic data provider.
//
// Config via subject string. Two syntaxes are accepted:
//
//	Query style:  "foo?period=7us&size=64&mode=const&burst=1&jitter=0.02&drop=1&ts=1&log=1"
//	Path style:   "foo/period/7us/size/64/mode/poisson/rate/120000/jitter/0.05/drop/0/ts/1/log/1"
//
// Parameters:
//
//	period:   Go duration. Inter-message target (wins over rate).
//	rate:     Messages per second. Used if period absent.
//	mode:     const | poisson | onoff
//	burst:    Messages emitted per tick (>=1).
//	jitter:   ±fraction jitter on period (e.g., 0.05 = ±5%).
//	on/off:   Durations for onoff mode (e.g., on=5ms&off=1ms).
//	size:     Payload bytes (>=1). If ts=1 and size<16, auto-extends to 16.
//	ptype:    bytes | counter | json  (payload content generator)
//	drop:     1=non-blocking send (drop on backpressure), 0=block.
//	ts:       1=prepend 16B header: [sendUnixNano int64][seq int64].
//	log:      1=emit per-second metrics via slog.
//
// Notes:
//   - Constant mode uses sleep-then-spin pacer for sub-10µs.
//   - Poisson mode draws inter-arrivals from Exp(rate).
//   - On/Off emits at period during "on", silent during "off" windows.
//   - Metrics include msgs/s, bytes/s, drops/s per stream.
//   - Fetch is unsupported (returns error).
package test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

type TestProvider struct {
	mu       sync.Mutex
	streams  map[string]*stream
	out      chan<- domain.Message
	defaults cfg
}

type stream struct {
	cancel context.CancelFunc
	done   chan struct{}
	stats  *metrics
}

type metrics struct {
	sent, dropped atomic.Uint64
	prevSent      uint64
	prevDropped   uint64
	startUnix     int64
}

type mode int

const (
	modeConst mode = iota
	modePoisson
	modeOnOff
)

type ptype int

const (
	ptBytes ptype = iota
	ptCounter
	ptJSON
)

type cfg struct {
	period      time.Duration // inter-arrival target
	rate        float64       // msgs/sec if period == 0
	jitter      float64       // ±fraction
	mode        mode
	onDur       time.Duration // for onoff
	offDur      time.Duration // for onoff
	burst       int
	size        int
	pType       ptype
	dropIfSlow  bool
	embedTS     bool
	logEverySec bool
}

// NewTestProvider returns a provider with sane defaults.
func NewTestProvider(out chan<- domain.Message, defaultPeriod time.Duration) *TestProvider {
	if defaultPeriod <= 0 {
		defaultPeriod = 100 * time.Microsecond
	}
	return &TestProvider{
		streams: make(map[string]*stream),
		out:     out,
		defaults: cfg{
			period:     defaultPeriod,
			rate:       0,
			jitter:     0,
			mode:       modeConst,
			onDur:      5 * time.Millisecond,
			offDur:     1 * time.Millisecond,
			burst:      1,
			size:       32,
			pType:      ptBytes,
			dropIfSlow: true,
			embedTS:    true,
		},
	}
}

func (p *TestProvider) Start() error { return nil }

func (p *TestProvider) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, s := range p.streams {
		s.cancel()
		<-s.done
		delete(p.streams, key)
	}
}

func (p *TestProvider) Subscribe(subject string) <-chan error {
	errCh := make(chan error, 1)

	if !p.IsValidSubject(subject, false) {
		errCh <- errors.New("invalid subject")
		close(errCh)
		return errCh
	}

	p.mu.Lock()
	if _, exists := p.streams[subject]; exists {
		p.mu.Unlock()
		errCh <- nil
		return errCh
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{
		cancel: cancel,
		done:   make(chan struct{}),
		stats:  &metrics{startUnix: time.Now().Unix()},
	}
	p.streams[subject] = s
	out := p.out
	conf := p.parseCfg(subject)
	p.mu.Unlock()

	go run(ctx, s, out, subject, conf)

	errCh <- nil
	return errCh
}

func (p *TestProvider) Unsubscribe(subject string) <-chan error {
	errCh := make(chan error, 1)

	p.mu.Lock()
	s, ok := p.streams[subject]
	if !ok {
		p.mu.Unlock()
		errCh <- errors.New("not subscribed")
		return errCh
	}
	delete(p.streams, subject)
	p.mu.Unlock()

	go func() {
		s.cancel()
		<-s.done
		errCh <- nil
	}()
	return errCh
}

func (p *TestProvider) Fetch(_ string) (domain.Message, error) {
	return domain.Message{}, fmt.Errorf("fetch not supported by provider")
}

func (p *TestProvider) GetActiveStreams() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	keys := make([]string, 0, len(p.streams))
	for k := range p.streams {
		keys = append(keys, k)
	}
	return keys
}

func (p *TestProvider) IsStreamActive(key string) bool {
	p.mu.Lock()
	_, ok := p.streams[key]
	p.mu.Unlock()
	return ok
}

func (p *TestProvider) IsValidSubject(key string, _ bool) bool {
	if key == "" {
		return false
	}
	// Accept anything parseable via parseCfg; fallback true.
	return true
}

// --- core ---

func run(ctx context.Context, s *stream, out chan<- domain.Message, subject string, c cfg) {
	defer close(s.done)

	ident, _ := domain.RawID("test_provider", subject)

	// Sanitize
	if c.burst < 1 {
		c.burst = 1
	}
	if c.size < 1 {
		c.size = 1
	}
	if c.embedTS && c.size < 16 {
		c.size = 16
	}
	if c.period <= 0 {
		if c.rate > 0 {
			c.period = time.Duration(float64(time.Second) / c.rate)
		} else {
			c.period = 10 * time.Microsecond
		}
	}
	if c.jitter < 0 {
		c.jitter = 0
	}
	if c.jitter > 0.95 {
		c.jitter = 0.95
	}

	// Per-second logging
	var logTicker *time.Ticker
	if c.logEverySec {
		logTicker = time.NewTicker(time.Second)
		defer logTicker.Stop()
	}

	var seq uint64
	base := make([]byte, c.size)

	// On/Off state
	onUntil := time.Time{}
	offUntil := time.Time{}
	inOn := true
	now := time.Now()
	onUntil = now.Add(c.onDur)

	// Scheduling
	next := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		switch c.mode {
		case modeConst:
			// sleep-then-spin to hit sub-10µs with isolated core
			if d := time.Until(next); d > 0 {
				if d > 30*time.Microsecond {
					time.Sleep(d - 30*time.Microsecond)
				}
				for time.Now().Before(next) {
				}
			}
		case modePoisson:
			// draw from exponential with mean=period
			lam := 1.0 / float64(c.period)
			ia := time.Duration(rand.ExpFloat64() / lam)
			next = time.Now().Add(ia)
			// No pre-wait here; emit immediately then sleep to next
		case modeOnOff:
			now = time.Now()
			if inOn {
				if now.After(onUntil) {
					inOn = false
					offUntil = now.Add(c.offDur)
					continue
				}
			} else {
				if now.After(offUntil) {
					inOn = true
					onUntil = now.Add(c.onDur)
				}
				// While off, push next and wait
				// Small sleep to avoid busy loop during off
				time.Sleep(minDur(c.offDur/4, 200*time.Microsecond))
				continue
			}
			// For on state, behave like const
			if d := time.Until(next); d > 0 {
				if d > 30*time.Microsecond {
					time.Sleep(d - 30*time.Microsecond)
				}
				for time.Now().Before(next) {
				}
			}
		}

		// Emit burst
		for i := 0; i < c.burst; i++ {
			seq++
			payload := base[:c.size]
			switch c.pType {
			case ptBytes:
				fillPattern(payload, uint64(seq))
			case ptCounter:
				fillCounter(payload, uint64(seq))
			case ptJSON:
				// build minimal, fixed-size-ish JSON into payload
				n := buildJSON(payload, uint64(seq))
				payload = payload[:n]
			}

			if c.embedTS {
				ensureCap(&payload, 16)
				ts := time.Now().UnixNano()
				putInt64(payload[0:8], ts)
				putInt64(payload[8:16], int64(seq))
			}

			msg := domain.Message{
				Identifier: ident,
				Payload:    payload,
			}

			if out != nil {
				if c.dropIfSlow {
					select {
					case out <- msg:
						s.stats.sent.Add(1)
					default:
						s.stats.dropped.Add(1)
					}
				} else {
					select {
					case out <- msg:
						s.stats.sent.Add(1)
					case <-ctx.Done():
						return
					}
				}
			}
		}

		// Schedule next
		adj := c.period
		if c.mode == modePoisson {
			// next already chosen
		} else {
			if c.jitter > 0 {
				j := (rand.Float64()*2 - 1) * c.jitter
				adj = time.Duration(float64(c.period) * (1 + j))
				if adj < 0 {
					adj = 0
				}
			}
			next = next.Add(adj)
		}

		// For poisson, actively wait to next
		if c.mode == modePoisson {
			if d := time.Until(next); d > 0 {
				if d > 30*time.Microsecond {
					time.Sleep(d - 30*time.Microsecond)
				}
				for time.Now().Before(next) {
				}
			}
		}
	}
}

// --- config parsing ---

func (p *TestProvider) parseCfg(subject string) cfg {
	c := p.defaults

	// Query style first
	if i := strings.Index(subject, "?"); i >= 0 && i < len(subject)-1 {
		if qv, err := url.ParseQuery(subject[i+1:]); err == nil {
			c = applyQuery(c, qv)
		}
	}

	// Path segments like /key/value/ pairs
	parts := strings.Split(subject, "/")
	for i := 0; i+1 < len(parts); i += 2 {
		k := strings.ToLower(parts[i])
		v := parts[i+1]
		if k == "" {
			continue
		}
		applyKV(&c, k, v)
	}
	return c
}

func applyQuery(c cfg, v url.Values) cfg {
	for k, vals := range v {
		if len(vals) == 0 {
			continue
		}
		applyKV(&c, strings.ToLower(k), vals[0])
	}
	return c
}

func applyKV(c *cfg, key, val string) {
	switch key {
	case "period":
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			c.period = d
		}
	case "rate":
		if f, err := strconv.ParseFloat(val, 64); err == nil && f > 0 {
			c.rate = f
			c.period = 0 // let rate take effect if period unset later
		}
	case "mode":
		switch strings.ToLower(val) {
		case "const", "steady":
			c.mode = modeConst
		case "poisson":
			c.mode = modePoisson
		case "onoff", "burst":
			c.mode = modeOnOff
		}
	case "on":
		if d, err := time.ParseDuration(val); err == nil && d >= 0 {
			c.onDur = d
		}
	case "off":
		if d, err := time.ParseDuration(val); err == nil && d >= 0 {
			c.offDur = d
		}
	case "burst":
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			c.burst = n
		}
	case "jitter":
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f < 1 {
			c.jitter = f
		}
	case "size":
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			c.size = n
		}
	case "ptype":
		switch strings.ToLower(val) {
		case "bytes":
			c.pType = ptBytes
		case "counter":
			c.pType = ptCounter
		case "json":
			c.pType = ptJSON
		}
	case "drop":
		c.dropIfSlow = val == "1" || strings.EqualFold(val, "true")
	case "ts":
		c.embedTS = val == "1" || strings.EqualFold(val, "true")
	case "log":
		c.logEverySec = val == "1" || strings.EqualFold(val, "true")
	}
}

// --- payload builders ---

func fillPattern(b []byte, seed uint64) {
	// xorshift for deterministic but non-trivial bytes
	if len(b) == 0 {
		return
	}
	x := seed | 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = byte(x)
	}
}

func fillCounter(b []byte, seq uint64) {
	for i := range b {
		b[i] = byte((seq + uint64(i)) & 0xFF)
	}
}

func buildJSON(buf []byte, seq uint64) int {
	// Small fixed fields. Truncate if buffer small.
	// Example: {"t":1694490000000000,"s":12345,"p":100.12}
	ts := time.Now().UnixNano()
	price := 10000 + float64(seq%1000)*0.01
	str := fmt.Sprintf(`{"t":%d,"s":%d,"p":%.2f}`, ts, seq, price)
	n := copy(buf, str)
	return n
}

func ensureCap(b *[]byte, need int) {
	if len(*b) >= need {
		return
	}
	nb := make([]byte, need)
	copy(nb, *b)
	*b = nb
}

func putInt64(b []byte, v int64) {
	_ = b[7]
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
