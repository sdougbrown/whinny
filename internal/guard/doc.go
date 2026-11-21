// Package guard is the send-path guard for the whinny link daemon: before
// a message is sent, the chain checks the breaker latch, the room's
// quiet state, and the token-bucket rate limit, in that order — first
// denial wins.
//
// Evaluate decides without side effects and consumes a bucket token only
// when the send is allowed, so a denial never drains the bucket. A
// non-positive PerMinute fails closed: every send is denied with
// refused-rate-limit.
//
// Denials are typed: refused-breaker (latched, do not retry; carries the
// breaker reason when set), refused-rate-limit (retryable, carries the
// wait until the next token), and refused-quiet (the room asked the bot
// to be quiet).
//
// Time is injected: the chain and bucket never call time.Now.
package guard
