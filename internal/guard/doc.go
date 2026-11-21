// Package guard is the send-path guard for the whinny link daemon: before
// a message is sent, the chain checks the breaker latch, the token-bucket
// rate limit, and the room's quiet state, in that order — first denial
// wins.
//
// Denials are typed: refused-breaker (latched, do not retry),
// refused-rate-limit (retryable, carries the wait until the next token),
// and refused-quiet (the room asked the bot to be quiet).
//
// Time is injected: the chain and bucket never call time.Now.
package guard
