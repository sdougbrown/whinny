package guard

import (
	"fmt"
	"sync"
	"time"
)

// Denial kinds.
const (
	// KindRefusedBreaker: the breaker is latched; do not retry.
	KindRefusedBreaker = "refused-breaker"
	// KindRefusedRateLimit: the rate bucket is empty; retry after
	// RetryAfter.
	KindRefusedRateLimit = "refused-rate-limit"
	// KindRefusedQuiet: the room asked the bot to be quiet.
	KindRefusedQuiet = "refused-quiet"
)

// Config configures a guard chain.
type Config struct {
	// PerMinute is the send rate limit: the token-bucket capacity and
	// the number of tokens refilled per minute. A non-positive value
	// yields a bucket that never admits a token (fail closed).
	PerMinute int
}

// Denial is a typed refusal from the guard chain.
type Denial struct {
	// Kind is one of the refused-* constants.
	Kind string
	// RetryAfter is non-zero only for rate-limit denials: the wait until
	// the next token.
	RetryAfter time.Duration
	// Text is a templated, actionable sentence.
	Text string
}

// GuardInput is everything a chain needs to evaluate one send.
type GuardInput struct {
	// BreakerLatched is the persisted breaker state.
	BreakerLatched bool
	// BreakerReason is the persisted breaker reason; when the breaker is
	// latched it is included in the refusal text.
	BreakerReason string
	// Bucket, when non-nil, overrides the chain's default bucket for
	// this evaluation.
	Bucket *RateBucket
	// QuietUntil is the room's quiet-until timestamp in unix seconds;
	// 0 means not quiet.
	QuietUntil int64
	// Now is the injected clock.
	Now time.Time
}

// Chain evaluates guards in fixed order (breaker, rate-limit, quiet); the
// first denial wins. Evaluate decides without side effects and consumes a
// bucket token only when the send is allowed, so a denial never drains
// the bucket. The chain owns the default rate bucket built from the
// config, so bucket state persists across Evaluate calls.
type Chain struct {
	bucket *RateBucket
}

// NewChain builds the guard chain from cfg.
func NewChain(cfg Config) *Chain {
	return &Chain{bucket: NewRateBucket(cfg)}
}

// Evaluate runs the guards in precedence order (breaker, rate-limit,
// quiet) without side effects and returns the first denial, if any. When
// the send is allowed it consumes one bucket token.
func (c *Chain) Evaluate(in GuardInput) (Denial, bool) {
	if d, denied := checkBreaker(in); denied {
		return d, true
	}
	if d, denied := checkRateLimit(c, in); denied {
		return d, true
	}
	if d, denied := checkQuiet(in); denied {
		return d, true
	}
	b := c.bucket
	if in.Bucket != nil {
		b = in.Bucket
	}
	if b != nil && b.cap > 0 {
		b.Take(in.Now)
	}
	return Denial{}, false
}

func checkBreaker(in GuardInput) (Denial, bool) {
	if !in.BreakerLatched {
		return Denial{}, false
	}
	text := "latched, do not retry"
	if in.BreakerReason != "" {
		text = fmt.Sprintf("latched (%s), do not retry", in.BreakerReason)
	}
	return Denial{Kind: KindRefusedBreaker, Text: text}, true
}

// checkRateLimit reports a denial when the bucket cannot serve a token
// now. It does not consume: a zero-capacity bucket (PerMinute <= 0)
// fails closed, and an empty bucket is already out of tokens.
func checkRateLimit(c *Chain, in GuardInput) (Denial, bool) {
	b := c.bucket
	if in.Bucket != nil {
		b = in.Bucket
	}
	if b == nil || b.cap <= 0 {
		return Denial{Kind: KindRefusedRateLimit, Text: "rate limit: no tokens available"}, true
	}
	if ok, wait := b.CanServe(in.Now); !ok {
		return Denial{
			Kind:       KindRefusedRateLimit,
			RetryAfter: wait,
			Text:       fmt.Sprintf("retry is safe after %s", wait),
		}, true
	}
	return Denial{}, false
}

func checkQuiet(in GuardInput) (Denial, bool) {
	if in.QuietUntil == 0 || in.Now.Unix() >= in.QuietUntil {
		return Denial{}, false
	}
	return Denial{Kind: KindRefusedQuiet, Text: "the room asked the bot to be quiet"}, true
}

// RateBucket is a token bucket: capacity = per-minute rate, refilled at one
// token per 60s/rate. Time is injected via Take and CanServe.
type RateBucket struct {
	mu     sync.Mutex
	tokens float64
	cap    float64
	refill time.Duration
	last   time.Time
	// onTake, when set, is called after every successful Take with the
	// bucket's current state; the hook's error is dropped (the in-memory
	// state stays authoritative for the process).
	onTake func(tokens float64, last time.Time) error
}

// NewRateBucket builds a bucket from cfg, starting full. A non-positive
// PerMinute yields a bucket that never admits a token.
func NewRateBucket(cfg Config) *RateBucket {
	if cfg.PerMinute <= 0 {
		return &RateBucket{}
	}
	return &RateBucket{
		tokens: float64(cfg.PerMinute),
		cap:    float64(cfg.PerMinute),
		refill: time.Minute / time.Duration(cfg.PerMinute),
	}
}

// NewPersistedBucket builds a RateBucket whose state loads at
// construction and saves after each successful Take. A load error or a
// never-set state (a zero last instant) leaves the bucket full; a save
// error is dropped (the in-memory state stays authoritative for the
// process). The bucket is a RateBucket, so it satisfies whatever the
// chain's GuardInput.Bucket expects.
func NewPersistedBucket(load func() (float64, time.Time, error), save func(float64, time.Time) error, perMinute int) *RateBucket {
	b := NewRateBucket(Config{PerMinute: perMinute})
	if tokens, last, err := load(); err == nil && !last.IsZero() {
		b.mu.Lock()
		b.tokens = tokens
		b.last = last
		b.mu.Unlock()
	}
	b.onTake = func(tokens float64, last time.Time) error {
		if save == nil {
			return nil
		}
		return save(tokens, last)
	}
	return b
}

// CanServe reports whether a token is available at now without consuming
// one. It refills first; the refill advances the bucket's clock but
// never takes a token.
func (b *RateBucket) CanServe(now time.Time) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap <= 0 {
		return false, 0
	}
	b.refillTo(now)
	if b.tokens >= 1-1e-9 {
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) * b.refill.Seconds() * float64(time.Second))
}

// Take consumes one token at now, refilling first. It reports whether a
// token was available and, if not, the wait until the next one.
func (b *RateBucket) Take(now time.Time) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap <= 0 {
		return false, 0
	}
	b.refillTo(now)
	if b.tokens >= 1-1e-9 {
		b.tokens--
		if b.onTake != nil {
			b.onTake(b.tokens, b.last)
		}
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) * b.refill.Seconds() * float64(time.Second))
}

// refillTo tops the bucket up for the time elapsed since the last
// refill. The caller must hold b.mu.
func (b *RateBucket) refillTo(now time.Time) {
	if !b.last.IsZero() && now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() / b.refill.Seconds()
		if b.tokens > b.cap {
			b.tokens = b.cap
		}
	}
	b.last = now
}
