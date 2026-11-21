// Package store is the SQLite persistence layer for the whinny link daemon.
//
// It holds the state the daemon must survive restarts: watches
// (edge/pattern/thread/subscription), the per-agent wake queue, the send-path
// breaker latch, per-room read cursors, and a small key/value meta table.
//
// The daemon is a single writer. All write methods are serialized by one
// mutex, so writes never contend even if called from multiple goroutines;
// reads may run concurrently.
//
// Time is injected: methods that record a timestamp take a time.Time (unix
// seconds in the schema). The package never calls time.Now.
package store
