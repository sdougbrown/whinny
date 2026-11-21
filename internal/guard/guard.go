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
// first denial wins. The chain owns the default rate bucket built from the
// config, so bucket state persists across Evaluate calls.
type Chain struct {
	bucket *RateBucket
	guards []guard
}

type guard struct {
	name  string
	check func(c *Chain, in GuardInput) (Denial, bool)
}

// NewChain builds the guard chain from cfg.
func NewChain(cfg Config) *Chain {
	return &Chain{
		bucket: NewRateBucket(cfg),
		guards: []guard{
			{name: "breaker", check: checkBreaker},
			{name: "rate-limit", check: checkRateLimit},
			{name: "quiet", check: checkQuiet},
		},
	}
}

// Evaluate runs the guards in order and returns the first denial, if any.
func (c *Chain) Evaluate(in GuardInput) (Denial, bool) {
	for _, g := range c.guards {
		if d, denied := g.check(c, in); denied {
			return d, true
		}
	}
	return Denial{}, false
}

func checkBreaker(_ *Chain, in GuardInput) (Denial, bool) {
	if !in.BreakerLatched {
		return Denial{}, false
	}
	return Denial{Kind: KindRefusedBreaker, Text: "latched, do not retry"}, true
}

func checkRateLimit(c *Chain, in GuardInput) (Denial, bool) {
	b := c.bucket
	if in.Bucket != nil {
		b = in.Bucket
	}
	if b == nil || b.cap == 0 {
		return Denial{}, false
	}
	ok, wait := b.Take(in.Now)
	if ok {
		return Denial{}, false
	}
	return Denial{
		Kind:       KindRefusedRateLimit,
		RetryAfter: wait,
		Text:       fmt.Sprintf("retry is safe after %s", wait),
	}, true
}

func checkQuiet(_ *Chain, in GuardInput) (Denial, bool) {
	if in.QuietUntil == 0 || in.Now.Unix() >= in.QuietUntil {
		return Denial{}, false
	}
	return Denial{Kind: KindRefusedQuiet, Text: "the room asked the bot to be quiet"}, true
}

// RateBucket is a token bucket: capacity = per-minute rate, refilled at one
// token per 60s/rate. Time is injected via Take.
type RateBucket struct {
	mu     sync.Mutex
	tokens float64
	cap    float64
	refill time.Duration
	last   time.Time
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

// Take consumes one token at now, refilling first. It reports whether a
// token was available and, if not, the wait until the next one.
func (b *RateBucket) Take(now time.Time) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap == 0 {
		return false, 0
	}
	if !b.last.IsZero() && now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() / b.refill.Seconds()
		if b.tokens > b.cap {
			b.tokens = b.cap
		}
	}
	b.last = now
	if b.tokens >= 1-1e-9 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) * b.refill.Seconds() * float64(time.Second))
}
