// Package daemon is the whinny long-running process: it holds the apron
// WebSocket connection, evaluates the wake policy over inbound records,
// persists watches and wakes through internal/store, and delivers wake
// envelopes through a pluggable backend (exec, resume, socket, or feed).
//
// Run drives everything; Config carries the resolved settings and Deps
// the injected client, store, deliverer, clock, and ticker.
//
// Mode resident is the full policy (mention, reply, watch, ambient gate)
// for one resident identity, backed by the durable wake queue. Mode
// shared drops mention/reply/ambient evaluation: it fires watch hits to
// their owners' delivery callbacks (store meta key "delivery:<owner>")
// with no queue and no redelivery — the transient run may be gone, and
// nothing is owed.
//
// This is v1 of the extraction from the wake.mjs reference: the RoomLink
// seam is the daemon struct itself. When the standalone link grows out
// of this package, it extracts from here.
package daemon
