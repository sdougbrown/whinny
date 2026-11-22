// Package daemon is the whinny long-running process: it holds the room
// transport connection, evaluates the wake policy over inbound events,
// persists watches and wakes through internal/store, and delivers wake
// envelopes through a pluggable backend (exec, resume, socket, or feed).
//
// Run drives everything; Config carries the resolved settings and Deps
// the injected link, store, deliverer, clock, and ticker.
//
// The room transport sits behind the Link interface (link.go): the apron
// adapter wraps an apron WebSocket client, the Slack adapter runs over
// Socket Mode and Slack's Web API. Everything transport-specific —
// connection shape, message-id and cursor semantics, mention sources —
// stays behind the implementations; the policy and delivery layers are
// transport-agnostic.
//
// Mode resident is the full policy (mention, reply, watch, ambient gate)
// for one resident identity, backed by the durable wake queue. Mode
// shared drops mention/reply/ambient evaluation: it fires watch hits to
// their owners' delivery callbacks (store meta key "delivery:<owner>")
// with no queue and no redelivery — the transient run may be gone, and
// nothing is owed.
package daemon
