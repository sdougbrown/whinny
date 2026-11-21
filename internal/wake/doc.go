// Package wake turns inbound room traffic into typed wake decisions.
//
// It is a pure package: no sqlite, no websocket. All I/O is behind
// interfaces (HistoryLookup, Classifier) and an injected Clock. The daemon
// owns the AmbientGate state and the watch list; this package only decides.
//
// The decision is an aggregation over an ordered evaluation:
//
//  1. Mention — the bot's name (or an alias) appears on a word boundary,
//     as an @name/@alias token, as a leading "name:"/"name," prefix, or in
//     the message's mentions array. Any sender wakes, humans and bots
//     alike; the evaluation stops there.
//  2. Bot firebreak — a bot sender that was not mentioned is eligible only
//     for watch matching: a match wakes with kind watch, otherwise the
//     message is ignored. Bots never wake the bot via reply or ambient.
//  3. Reply-to-my-message — a human message that replies to one of the
//     bot's own messages, resolved from room history, never in-memory
//     state.
//  4. Watch match — every registered watch (edge, pattern, thread, or
//     subscription) that matches fires: all of them appear in
//     Decision.Watches and their once-ids in Decision.RemoveWatchIDs.
//  5. Ambient gate — a human sender that matched nothing above is passed
//     to the Classifier, bounded by the AmbientGate valves (min spacing,
//     hourly cap). The gate fails open on classifier error.
//
// Kind is the highest-priority fired path (mention > reply > watch >
// ambient); Wake is true if any path fired; Reason comes from the primary
// path.
package wake
