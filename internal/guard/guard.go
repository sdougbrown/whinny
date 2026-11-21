package guard

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sdougbrown/whinny/internal/rate"
	"github.com/sdougbrown/whinny/internal/store"
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

// Chain evaluates guards in fixed order (breaker, quiet, rate-limit); the
// first denial wins. Evaluate decides without side effects and consumes a
// bucket token only when the send is allowed, so a denial never drains
// the bucket. Quiet is checked before the rate take precisely so a
// quiet denial never consumes a token. The chain owns the default rate
// bucket built from the config, so bucket state persists across
// Evaluate calls.
type Chain struct {
	bucket *RateBucket
}

// NewChain builds the guard chain from cfg.
func NewChain(cfg Config) *Chain {
	return &Chain{bucket: NewRateBucket(cfg)}
}

// Evaluate runs the guards in precedence order (breaker, quiet,
// rate-limit) without side effects and returns the first denial, if any.
// When the send is allowed it consumes one bucket token.
func (c *Chain) Evaluate(in GuardInput) (Denial, bool) {
	if d, denied := checkBreaker(in); denied {
		return d, true
	}
	if d, denied := checkQuiet(in); denied {
		return d, true
	}
	// Rate limit comes last because a store-backed bucket consumes its
	// token inside the check (one atomic transaction); checking quiet
	// first keeps a quiet denial from consuming a token. When the bucket
	// is empty AND the room is quiet, the caller sees refused-quiet.
	if d, denied := checkRateLimit(c, in); denied {
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
	// takeFn, when set, makes the take perform the (refill, take,
	// persist) as one atomic operation. This is the store-backed path
	// (NewStoreBucket): the check and the consume are one transaction, so
	// a denial never leaves the bucket drained and a take is never lost
	// to a concurrent caller. A persistence error denies (fail closed)
	// with a 1s retry-after. The chain checks breaker and quiet BEFORE
	// the take, so a quiet denial never consumes a token.
	takeFn func(now time.Time) (ok bool, retryAfter time.Duration, err error)
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

// NewStoreBucket builds a RateBucket backed by the store's atomic
// TakeRateToken: the (refill, take, persist) is one BEGIN IMMEDIATE
// transaction, so N concurrent callers can never all pass a perMinute
// limit, and a persistence error denies (fail closed) with a 1s
// retry-after. The chain checks quiet before the rate take precisely
// so a quiet denial never consumes a token. The store call runs on
// context.Background: the bucket's API carries no context.
func NewStoreBucket(s *store.Store, perMinute int) *RateBucket {
	b := NewRateBucket(Config{PerMinute: perMinute})
	b.takeFn = func(now time.Time) (bool, time.Duration, error) {
		return s.TakeRateToken(context.Background(), perMinute, now)
	}
	return b
}

// CanServe reports whether a token is available at now without consuming
// one. It refills first; the refill advances the bucket's clock but
// never takes a token. On a store-backed bucket (takeFn set), CanServe
// is the atomic take: it refills, takes, and persists in one
// transaction, and a persistence error denies (fail closed) with a 1s
// retry-after.
func (b *RateBucket) CanServe(now time.Time) (bool, time.Duration) {
	if b.takeFn != nil {
		ok, wait, err := b.takeFn(now)
		if err != nil {
			return false, time.Second
		}
		return ok, wait
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap <= 0 {
		return false, 0
	}
	st, _ := rate.Refill(rate.State{Tokens: b.tokens, Last: b.last}, int(b.cap), now)
	b.tokens, b.last = st.Tokens, st.Last
	ok, wait := rate.Available(st, int(b.cap))
	return ok, wait
}

// Take consumes one token at now, refilling first. It reports whether a
// token was available and, if not, the wait until the next one. On a
// store-backed bucket (takeFn set), Take is a no-op: the take already
// happened in CanServe.
func (b *RateBucket) Take(now time.Time) (bool, time.Duration) {
	if b.takeFn != nil {
		return true, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap <= 0 {
		return false, 0
	}
	st, _ := rate.Refill(rate.State{Tokens: b.tokens, Last: b.last}, int(b.cap), now)
	ok, wait := rate.Available(st, int(b.cap))
	if ok {
		st = rate.Take(st)
		b.tokens, b.last = st.Tokens, st.Last
	}
	return ok, wait
}
